use std::sync::{
    Arc,
    Weak,
    Mutex,
};
use std::net::SocketAddr;
use std::path::Path;
use std::io::{
    Result as IOResult,
    Error as IOError,
    ErrorKind as IOErrorKind,
};

use rand::Rng;

use rmp_serde::decode;
use rmp_serde::encode;

use log::trace;

use rpcudp_rs::{RpcServer, rpc};

use super::routing::{
    key::Key,
    peer::PeerAddress,
    rtable::{
	RoutingTable,
	RTDiskV1,
    }
};


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
	async fn ping(&self, context: RpcContext) {
	    let _node = self.node();
	    trace!("Ping from {}", context.source);
	}

	async fn store(&self, context: RpcContext) {
	    let _node = self.node();
	    trace!("Store from {}", context.source);
	}

	async fn find_node(&self, context: RpcContext) {
	    let _node = self.node();
	    trace!("Store from {}", context.source);
	}

	async fn find_value(&self, context: RpcContext) {
	    let _node = self.node();
	    trace!("Store from {}", context.source);
	}
    }
}


type NodeMut = Mutex<NodeStruct>;
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
	inner.rpc.replace(rpc);
	Ok(())
    }

    pub fn node_key(&self) -> PeerAddress {
	self.0.lock().and_then(|s| Ok(s.rtable.node())).unwrap()
    }


    /// Bootstrap node from other node
    /// By bootstrapping the node, this node will re-initialize it's routing
    /// table and join the DHT network known by the other node.
    pub async fn bootstrap(&self, other: SocketAddr) {
	todo!();
    }
}


struct NodeStruct {
    rtable: RoutingTable,
    rpc: Option<RpcServer<NodeService>>
}


impl NodeStruct {
    fn new(rtable: RoutingTable) -> NodeStruct {
	NodeStruct {
	    rtable,
	    rpc: None,
	}
    }
}


#[cfg(test)]
mod test {
    use std::path::Path;
    use rand::{
	SeedableRng,
	rngs::SmallRng,
    };
    use tempfile::TempDir;

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
}