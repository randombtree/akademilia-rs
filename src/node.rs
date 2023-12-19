use std::sync::{
    Arc,
    Weak,
    Mutex,
};
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::Path;
use std::io::{
    Result as IOResult,
    Error as IOError,
    ErrorKind as IOErrorKind,
};
use std::time::Duration;
use std::pin::Pin;
use std::future::Future;

use futures::future::select_all;
use futures::future::{Fuse, FutureExt};
use futures::select;

use rand::Rng;

use serde::{Deserialize, Serialize};
use rmp_serde::decode;
use rmp_serde::encode;

use log::{
    info,
    trace,
};

use rpcudp_rs::{
    compat::time::timeout,
    RpcServer, rpc
};

use crate::routing::{
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

	async fn store(&self, context: RpcContext, node_key: Key, key: Key, data: Vec<u8>) {
	    let node = self.node()
		.expect("Node disappeared");
	    trace!("store from {}", context.source);
	    // TODO: Store stub
	    node.0.lock().map(|mut node| {
		node.store.insert(key, data);
	    });
	}

	async fn find_node(&self, context: RpcContext, node_key: Key, find_key: Key) -> Vec<NodeAddress> {
	    trace!("find_node from {}", context.source);
	    let node = self.node()
		.expect("Node disappeared");
	    node.0.lock().map(|mut node| {
		node.update_peer_seen(context.source, node_key);
		node.find_node(&find_key)
	    }).expect("Memory corruption")
	}

	async fn find_value(&self, context: RpcContext) {
	    trace!("find_value from {}", context.source);
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

    fn push(&mut self, peer: Peer) {
	self.peers.insert(SpiderPeer::new(peer));
    }

    fn extend<I>(&mut self, iter: I)
	where I: IntoIterator<Item = Peer>
    {
	self.peers.extend(
	    iter.into_iter().map(|peer| SpiderPeer::new(peer))
	);
    }

    fn add_finders<'a>(&'a mut self, count: usize) -> impl Iterator<Item=Pin<Box<impl Future<Output=Option<Vec<Peer>>>>>> + 'a {
	self.peers.iter()
	    .filter(|p| !p.has_failed())   // Ignore failed nodes
	    .take(KAD_K)  // We continue until the KAD_K best nodes are probed
	    .filter(|p| !p.has_visited())
	    .take(count)  // Ensure max alpha ops
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
		    } else {
			spider_peer.set_finished();
		    }
		    ret
		})
	    })
    }

    /// Store value to K closest peers
    pub async fn store(&mut self, value: Vec<u8>) -> usize {
	trace!("Storing");
	let node_key = self.node.node_key();
	let mut finders = Some(Vec::new()); // Is none while awaiting future to finish
	let mut finders_count = 0; // Need to store length outside vec as future takes it
	let mut finders_finished = false;  // Once no more new nodes can be found, stop looking
	let mut storers = Some(Vec::new());
	let mut storers_count = 0;
	let mut finders_fut = Fuse::terminated();
	let mut storers_fut = Fuse::terminated();
	let mut successfully_stored = 0;

	loop {
	    // First, use all concurrency to search closer nodes (=routing)
	    let add_finders = KAD_ALPHA - finders_count - storers_count;
	    if !finders_finished && finders.is_some() && add_finders > 0  {
		let mut finders = finders.take().unwrap();
		finders.extend(self.add_finders(add_finders));
		finders_count = finders.len();
		// select_all panics if iterator is empty :)
		if finders_count > 0 {
		    finders_fut = select_all(finders.into_iter()).fuse();
		} else {
		    // Didn't find more nodes, don't bother searching the list anymore
		    finders_finished = true;
		}
	    }

	    // Once not all finders are busy, we can start storing
	    // Theoretically it can result in inserting to more than K nodes if a good node is
	    // found from a slow peer in the end of the routing
	    let add_storers = KAD_ALPHA - finders_count - storers_count;
	    if storers.is_some() &&  add_storers > 0 {
		let mut storers = storers.take().unwrap();
		storers.extend(
		    self.peers.iter()
			.filter(|p| !p.has_failed())  // Ignore failed ones;
			.take(KAD_K)                  // Store to the best K nodes
			.filter(|p| p.has_finished() && !p.has_stored())
			.take(add_storers)            // Obey concurrency
			.map(|p| {
			    p.set_stored();
			    p.clear_finished();  // New round
			    // Shed any references
			    let target = (*self.target).clone();
			    let spider_peer = p.clone();
			    let node = self.node.clone();
			    let value = value.clone(); // TODO: Make rpcudp take value by reference
			    Box::pin(async move {
				let ret = node.store_on(spider_peer.peer(), target, value).await;
				if ret.is_none() {
				    trace!("Store failed");
				    spider_peer.set_failed();
				} else {
				    spider_peer.set_finished();
				}
				ret
			    })
			}));
		storers_count = storers.len();
		// select_all panics if iterator is empty :)
		if storers_count > 0 {
		    storers_fut = select_all(storers.into_iter()).fuse();
		}
	    }

	    if finders_count + storers_count == 0 {
		// No work more to be done
		trace!("Finished: stored into {} nodes", successfully_stored);
		break successfully_stored
	    }

	    // Run pending futures
	    select! {
		(resolved, _index, new_finders) = finders_fut => {
		    finders_count -= 1;
		    if let Some(list) = resolved {
			self.peers.extend(
			    list.into_iter()
				.filter(|peer| peer.address != node_key)
				.map(|peer| SpiderPeer::new(peer)));
		    }
		    finders = Some(new_finders);
		},
		(resolved, _index, new_storers) = storers_fut => {
		    storers_count -= 1;
		    if resolved.is_some() {
			successfully_stored += 1;
			trace!("Progress: stored into {} node(s)", successfully_stored);
		    }
		    // The future does the peer marking, nothing other needs to be done..
		    storers = Some(new_storers);
		}
	    }
	}
    }

    /// Find K closest peers
    pub async fn find_node(&mut self) -> Vec<Peer> {
	trace!("Starting find_node spider");
	let node_key = self.node.node_key();
	let mut finders = Vec::new();
	loop {
	    let add = KAD_ALPHA - finders.len();
	    if add > 0 {
		finders.extend(self.add_finders(add))
	    }

	    if finders.len() == 0 {
		let found: Vec<_> = self.peers.iter()
		    .filter(|p| !p.has_failed())
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
			.filter(|peer| peer.address != node_key)
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

    /// Store (key, value) into KAD network.
    pub async fn store<K>(&self, key: K, value: Vec<u8>) -> Result<usize, ()>
    where K: Into<Key>,
    {
	let key = key.into();
	let nodes = self.0.lock().map(|node| node.rtable.find_node(&key))
	    .expect("Memory corruption");
	if nodes.len() == 0 {
	    info!("Node has no known routing peers, needs bootstrapping!");
	    return Err(()) // TODO: Error
	}
	let key = Arc::new(key);
	let mut spider = NodeSpider::new(self.clone(), key.clone());
	spider.extend(nodes);
	Ok(spider.store(value).await)
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

    pub(crate) async fn store_on(&self, peer: &Peer, key: Key, value: Vec<u8>) -> Option<()> {
	let (rpc, node_key) = self.0.lock().map(|node| {
	    let rpc = node.rpc.clone().expect("Bug: Node not initialized");
	    (rpc, (*node.rtable.node()).clone())
	}).expect("Memory corruption");
	let peer_socket = peer.peer();
	peer.update_try();
	timeout(Duration::from_millis(MAX_RTT_MS),
		rpc.store(peer_socket, node_key, key, value)).await
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
    }
}


struct NodeStruct {
    rtable: RoutingTable,
    /// Store stub (TODO)
    store: HashMap<Key, Vec<u8>>,
    rpc: Option<Arc<RpcServer<NodeService>>>
}


impl NodeStruct {
    fn new(rtable: RoutingTable) -> NodeStruct {
	NodeStruct {
	    rtable,
	    store: HashMap::new(),
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
    use std::collections::HashMap;

    use futures::future::join_all;

    use rand::{
	SeedableRng,
	rngs::SmallRng,
    };
    use tempfile::TempDir;
    use test_log::test;
    use log::trace;

    use super::Node;
    use crate::routing::key::Key;
    use crate::routing::constants::KAD_K;

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
    fn test_node_minimal_functionality() {
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

	    // Test storing from each node
	    trace!("Begin insert...");
	    let mut keys = HashMap::new();
	    let results = join_all(
		nodes.iter()
		    .enumerate()
		    .map(|(ndx, node)| {
			 let key_str = format!("Key {}", ndx);
			 let key = Key::from(key_str.as_str());
			 keys.insert(key.clone(), (0, key_str.clone()));
			 Box::pin(async move {
			     trace!("{} Insert key", ndx);
			     let mut data = Vec::new();
			     data.extend_from_slice(key_str.as_bytes());
			     node.store(key, data).await
			 })
		    })).await;
	    // Check that all stores succeeded - i.e. at least K stores per key.
	    for (ndx, result) in results.into_iter().enumerate() {
		assert!(result.is_ok(), "Node {} failed", ndx);
		let count = result.unwrap();
		assert!(count >= KAD_K, "Node {} only stored to {} nodes", ndx, count);
	    }
	    // Validate and count all node contents
	    for node in nodes.iter() {
		for (key, val) in node.0.lock().unwrap().store.iter() {
		    let check = keys.get_mut(key)
			.expect("Key missing? Store contains wrong key!");
		    assert!(val == check.1.as_bytes(), "Key {} not correctly stored", check.1);
		    check.0 += 1;
		}
	    }
	    // .. should be at least K copies of each key
	    for (count, keystr) in keys.values() {
		assert!(*count >= KAD_K && *count <= KAD_K + 2, "Key {} only had {} copies", keystr, count);
	    }
	})
    }
}
