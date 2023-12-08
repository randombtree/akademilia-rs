use std::sync::{
    Arc,
    Weak,
    Mutex,
};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::Path;
use std::io::{
    Result as IOResult,
    Error as IOError,
    ErrorKind as IOErrorKind,
};
use std::time::Duration;

use futures::future::select_all;

use rand::Rng;

use serde::{Deserialize, Serialize};
use rmp_serde::decode;
use rmp_serde::encode;

use log::trace;

use rpcudp_rs::{
    compat::time::timeout,
    RpcServer, rpc
};

use super::routing::{
    constants::{
	KAD_ALPHA,
	KAD_K,
	MAX_RTT_MS,
    },
    key::Key,
    peer::{Peer, PeerAddress},
    rtable::{
	RoutingTable,
	RTDiskV1,
    }
};

mod spider;
use self::spider::SpiderPeer;

/// Kad RPC service
struct NodeService {
    node: Weak<NodeMut>
}


impl NodeService {
    fn new(node: Weak<NodeMut>) -> NodeService {
	NodeService {
	    node,
	}
    }

    fn node(&self) -> Option<Node> {
	self.node.upgrade().and_then(|m| Some(Node(m)))
    }
}


rpc! {
    NodeService {
	async fn ping(&self, context: RpcContext, node_key: Key) -> Key {
	    trace!("Ping from {}", context.source);
	    let node = self.node()
		.expect("Node disappeared?"); // Fixme: Check teardown on udprpc
	    node.0.lock().map(|mut node| {
		node.update_peer_seen(context.source, node_key);
		*node.rtable.node().clone()
	    }).expect("Memory corruption")
	}

	async fn store(&self, context: RpcContext) {
	    let _node = self.node();
	    trace!("Store from {}", context.source);
	}

	async fn find_node(&self, context: RpcContext, node_key: Key, find_key: Key) -> Vec<NodeAddress> {
	    trace!("Store from {}", context.source);
	    let node = self.node()
		.expect("Node disappeared");
	    node.0.lock().map(|mut node| {
		node.update_peer_seen(context.source, node_key);
		node.find_node(&find_key)
	    }).expect("Memory corruption")
	}

	async fn find_value(&self, context: RpcContext) {
	    trace!("Store from {}", context.source);
	    let _node = self.node()
		.expect("Node disappeared");

	}
    }
}


#[derive(Serialize, Deserialize)]
struct NodeAddress {
    address: Key,
    peer: SocketAddr,
}


struct NodeSpider {
    node: Node,
    target: PeerAddress,
    peers: BTreeSet<SpiderPeer>
}


impl NodeSpider {
    fn new(node: Node, target: PeerAddress) -> NodeSpider {
	NodeSpider {
	    node,
	    target,
	    peers: BTreeSet::new(),
	}
    }

    fn push(&mut self, peer :Peer) {
	self.peers.insert(SpiderPeer::new(peer));
    }


    pub async fn find_node(&mut self) -> Vec<Peer> {
	trace!("Starting find_node spider");
	let mut finders = Vec::new();
	loop {
	    let add: i32 = KAD_ALPHA - i32::try_from(finders.len()).unwrap();
	    if add > 0 {
		finders.extend(
		    self.peers.iter()
			.filter(|p| !p.failed())   // Ignore failed nodes
			.take(KAD_K)  // We continue until the KAD_K best nodes are probed
			.filter(|p| !p.visited())
			.take(add.try_into().unwrap())  // Ensure max alpha ops
			.map(|p| {
			    p.set_visited();
			    // Shed any references
			    let target = (*self.target).clone();
			    let spider_peer = p.clone();
			    let node = self.node.clone();
			    Box::pin(async move {
				let ret = node.find_node(spider_peer.peer(), target).await;
				if ret.is_none() {
				    trace!("Peer {} find_node failed", spider_peer.peer().peer());
				    // This node shouldn't count towards the KAD_K nodes returned
				    spider_peer.set_failed();
				}
				ret
			    })
			})
		);
	    }
	    if finders.len() == 0 {
		let found: Vec<_> = self.peers.iter()
		    .filter(|p| !p.failed())
		    .take(KAD_K)
		    .map(|p| p.peer().clone())
		    .collect();
		trace!("find_node found {} nodes", found.len());
		break found;
	    }
	    let resolved;
	    let _index;
	    (resolved, _index, finders) = select_all(finders.into_iter()).await;
	    if let Some(list) = resolved {
		self.peers.extend(
		    list.into_iter()
			.map(|peer| SpiderPeer::new(peer))
		);
	    }
	}
    }
}


type NodeMut = Mutex<NodeStruct>;

#[derive(Clone)]
pub struct Node(Arc<NodeMut>);


impl Node {
    const NODE_DATA: &'static str = "node.dat";
    /// Create new node, with data at `data_dir`
    pub fn create<P: AsRef<Path>, R: Rng>(data_dir: P, rng: &mut R) -> IOResult<Node> {
	let data_dir: &Path = data_dir.as_ref();
	if !data_dir.exists() {
	    std::fs::create_dir(data_dir)?;
	} else if !data_dir.is_dir() {
	    // TODO: Enable when it stabilizes
	    // Currently requires #![feature(io_error_more)]
	    // return Err(IOError::new(IOErrorKind::NotADirectory, format!("Expected {} to be a directory!", data_dir.display())))
	    return Err(IOError::new(IOErrorKind::Other, format!("Expected {} to be a directory!", data_dir.display())))
	}

	let key = Arc::new(Key::from_random(rng));
	let rtable = RoutingTable::new(key);
	let data_file = data_dir.join(Self::NODE_DATA);
	std::fs::File::create(&data_file)
	    .and_then(|mut f| {
		encode::write(&mut f, &rtable.serialize())
		    .map_err(|e| IOError::new(IOErrorKind::Other, format!("Failed to write node key to disk {}", e)))
		    .or_else(|e| {
			// Remove key file if we failed to write to it..
			let _ = std::fs::remove_file(&data_file);
			Err(e)
		    })
	    })?;

	Ok(Node(Arc::new(Mutex::new(NodeStruct::new(rtable)))))
    }

    /// Loads the Node from old settings in datadir
    pub fn open<P: AsRef<Path>>(data_dir: P) -> IOResult<Node>  {
	let data_dir: &Path = data_dir.as_ref();

	let data_file = data_dir.join(Self::NODE_DATA);
	let rtable: RTDiskV1 = std::fs::File::open(&data_file)
	    .and_then(|f| {
		decode::from_read(f).map_err(|e| {
		    IOError::new(IOErrorKind::Other, format!("Node key file is corrupted: {}", e))
		})
	    })?;
	let rtable = rtable.into();
	Ok(Node(Arc::new(Mutex::new(NodeStruct::new(rtable)))))

    }

    /// Start RPC server on address
    /// Only one address can be active at a time
    pub async fn start(&self, addr: SocketAddr) -> IOResult<()> {
	let mut inner = self.0.lock().unwrap();
	let rpc = RpcServer::bind(addr,
				  NodeService::new(Arc::downgrade(&self.0))).await?;
	inner.rpc.replace(Arc::new(rpc));
	Ok(())
    }

    /// Get the local address of the node
    pub fn local_addr(&self) -> Option<SocketAddr> {
	self.0.lock().map(|node| node.rpc.as_deref().map(|rpc| rpc.local_addr()))
	    .expect("Memory corruption")
    }

    pub fn node_key(&self) -> PeerAddress {
	self.0.lock().and_then(|s| Ok(s.rtable.node())).unwrap()
    }

    /// Try to ping other node.
    pub async fn ping(&self, other: SocketAddr) -> Result<PeerAddress, ()> {
	let node = self.0.lock().unwrap();
	let rpc = node.rpc.as_ref().map(|rpc| rpc.clone())
	    .ok_or(())?;   // Fixme: Own error for stopped node
	let node_key = *node.rtable.node().clone();
	// Let go of lock during call
	drop(node);
	let other_key = timeout(Duration::from_millis(MAX_RTT_MS),
				rpc.ping(other, node_key))
	    .await
	    .map_err(|_| { trace!("Peer {} timed out", other);() })?
	    .map_err(|_| { trace!("RPC to {} encountered errors", other); () })?;
	self.0.lock().map(|mut node| {
	    let peer = node.rtable.get_peer(other, other_key);
	    peer.update_seen();
	    peer.update_try();
	    Ok(peer.address())
	}).expect("Memory corruption")
    }

    /// Bootstrap node from other node
    /// By bootstrapping the node, this node will re-initialize it's routing
    /// table and join the DHT network known by the other node.
    pub async fn bootstrap(&self, other: SocketAddr) -> Result<(),()>{
	trace!("Bootstrap to {}", other);
	// TODO: Should the rtable be cleared on bootstrap?
	let address = {
	    let mut tries = 3;
	    loop {
		tries -= 1;
		if let Ok(address) = self.ping(other).await {
		    break Ok(address)
		} else if tries == 0 {
		    break Err(())
		}
	    }
	}?; // Peer doesn't answer
	let peer = self.0.lock().map(|node| node.rtable.find_peer(&address))
	    .expect("Memory corruption")
	    .ok_or(())?; // Peer disappeared, busy bucket or something?

	// Try to find K closest nodes to our address
	let mut spider = NodeSpider::new(self.clone(), self.node_key());
	spider.push(peer);
	let _ = spider.find_node().await;

	Ok(())
    }

    pub(crate) async fn find_node(&self, peer: &Peer, key: Key) -> Option<Vec<Peer>> {
	let (rpc, node_key) = self.0.lock().map(|node| {
	    let rpc = node.rpc.clone().expect("Bug: Node not initialized");
	    (rpc, (*node.rtable.node()).clone())
	}).expect("Memory corruption");
	let peer_socket = peer.peer();
	peer.update_try();
	trace!("find_node to {}", peer_socket);
	timeout(Duration::from_millis(MAX_RTT_MS),
		rpc.find_node(peer_socket, node_key, key)).await
	    .inspect_err(|_| {
		trace!("Peer {} timed out on find_node", peer_socket);
		peer.inc_errors();
	    })
	    .map(|ret| {
		ret.inspect_err(|_| {
		    trace!("Peer {} RPC error", peer_socket);
		    peer.inc_errors();
		}).ok()
	    }).ok().flatten()
	    .inspect(|_| peer.update_seen())
	    .map(|list| {
		self.0.lock().map(|mut node| {
		    list.into_iter()
			.filter(|node_address| node_address.address != node_key)   // Our address
			.map(|node_address| {
			    node.rtable.get_peer(node_address.peer, node_address.address)
			}).collect()
		}).expect("Memory corruption")
	    })
    }
}


struct NodeStruct {
    rtable: RoutingTable,
    rpc: Option<Arc<RpcServer<NodeService>>>
}


impl NodeStruct {
    fn new(rtable: RoutingTable) -> NodeStruct {
	NodeStruct {
	    rtable,
	    rpc: None,
	}
    }

    fn update_peer_seen(&mut self, peer: SocketAddr, key: Key) {
	let peer = self.rtable.get_peer(peer, key);
	peer.update_seen();
    }

    fn find_node(&self, key: &Key) -> Vec<NodeAddress> {
	self.rtable.find_node(key).into_iter().map(|peer| {
	    NodeAddress {
		address: *peer.address,
		peer: peer.peer(),
	    }
	}).collect()
    }
}


#[cfg(test)]
mod test {
    use std::path::Path;
    use std::net::SocketAddr;

    use rand::{
	SeedableRng,
	rngs::SmallRng,
    };
    use tempfile::TempDir;
    use test_log::test;

    use super::Node;


    fn create_node<P: AsRef<Path>>(path: P) -> Node {
	let mut rng = SmallRng::from_entropy();
	Node::create(path, &mut rng).unwrap()
    }

    #[test]
    fn test_create_node() {
	let tmp_dir = TempDir::with_prefix_in("test_create_node-", ".").unwrap();
	let _node = create_node(&tmp_dir);
    }

    #[test]
    fn test_open_node() {
	let tmp_dir = TempDir::with_prefix_in("test_open_node-", ".").unwrap();
	let node = create_node(&tmp_dir);
	let nodekey = node.node_key();
	drop(node);

	let node = Node::open(&tmp_dir).unwrap();
	let key = node.node_key();
	assert!(nodekey == key);
    }

    #[test]
    fn test_start_node() {
	let tmp_dir = TempDir::with_prefix_in("test_start_node-", ".").unwrap();
	let node = create_node(&tmp_dir);
	rpcudp_rs::compat::task::block_on(async {
	    node.start("127.0.0.1:30000".parse().unwrap()).await.unwrap();
	})
    }

    struct NodeCreator {
	prefix: &'static str,
	index: usize,
    }

    impl NodeCreator {
	pub fn new(prefix: &'static str) -> NodeCreator {
	    NodeCreator {
		prefix,
		index: 0,
	    }
	}
    }

    impl Iterator for NodeCreator {
	type Item = (TempDir, Node);
	fn next(&mut self) -> Option<Self::Item> {
	    let tmp = TempDir::with_prefix_in(format!("{}_{}", self.prefix, self.index),
					      ".").unwrap();
	    let node = create_node(&tmp);
	    Some((tmp, node))
	}
    }

    async fn start_nodes(nodes: &Vec<Node>) {
	let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
	for node in nodes.iter() {
	    node.start(addr).await.unwrap();
	}
    }

    #[test]
    fn test_node_minimal_communication() {
	let (_tmp, nodes): (Vec<_>, Vec<_>) = NodeCreator::new("test_node_comm")
	    .take(2)
	    .unzip();

	rpcudp_rs::compat::task::block_on(async {
	    start_nodes(&nodes).await;
	    let _peer = nodes[0].ping(nodes[1].local_addr().unwrap()).await
		.expect("Failed to ping other node");
	})
    }

    #[test]
    fn test_node_minimal_bootstrap() {
	const NODE_COUNT: usize = 40;
	let (_tmp, nodes): (Vec<_>, Vec<_>) = NodeCreator::new("test_node_bootstrap")
	    .take(NODE_COUNT)
	    .unzip();

	rpcudp_rs::compat::task::block_on(async {
	    start_nodes(&nodes).await;
	    // Test bootstrapping through common hub node
	    let hub_addr = nodes[0].local_addr().unwrap();

	    // Do it sequentially here, to decrease randomness in the test
	    for node in nodes.iter().skip(1) {
		node.bootstrap(hub_addr).await.unwrap();
	    }

	    // Now all nodes should have at least 20 nodes in the rtable
	    let routes: Vec<Vec<_>> = nodes.iter().map(|node| {
		node.0.lock().map(|node| {
		    node.rtable.iter_buckets()
			.map(|bucket| bucket.iter().map(|p| (*p).clone()))
			.flatten()
			.collect()
		}).unwrap()
	    }).collect();

	    for (i, routes) in routes.iter().enumerate() {
		assert!(routes.len() >= 20, "Too few routes on node {}", i);
	    }
	})
    }
}
