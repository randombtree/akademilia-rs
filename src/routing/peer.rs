use std::net::SocketAddr;
use std::time::Instant;
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;


use super::key::*;


pub type Peer = Arc<RoutingPeer>;
pub type PeerAddress = Arc<Key>;


pub struct RoutingPeer {
    /// The KAD address to the Node
    pub(crate) address: PeerAddress,
    /// The XOR distance to node
    pub(crate) distance: Distance,
    /// Mutable fields
    mutable: Mutex<RoutingPeerMutable>,
    /// Error since last successful op:
    /// Note: Errors in own atomic field; avoid locking in bucket insert path.
    ///       Races are not critical per-se either..
    errors: AtomicU32,
}


impl RoutingPeer {
    pub fn new(node: &Key, peer: SocketAddr, address: Key) -> RoutingPeer {
	let distance = node.distance(&address);
	let now = Instant::now();
	let mutable = RoutingPeerMutable {
	    peer,
	    alter_peer: None,
	    last_seen: now,
	    last_try: now,
	};
	RoutingPeer {
	    address: Arc::new(address),
	    distance,
	    mutable: Mutex::new(mutable),
	    errors: AtomicU32::new(0),
	}
    }

    /// Update last_seen (and clear errors)
    pub fn update_seen(&self) {
	let mut mutable = self.mutable.lock().unwrap();
	mutable.last_seen = Instant::now();
	self.errors.store(0, Ordering::Relaxed);
    }

    /// Update last_try
    pub fn update_try(&self) {
	self.errors.store(0, Ordering::Relaxed);
	let mut mutable = self.mutable.lock().unwrap();
	mutable.last_try = Instant::now();
    }

    /// Increase error count
    #[inline(always)]
    pub fn inc_errors(&self) {
	self.errors.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn get_errors(&self) -> u32 {
	self.errors.load(Ordering::Relaxed)
    }

    #[inline(always)]
    pub fn has_errors(&self) -> bool { self.get_errors() > 0 }
}


impl Default for RoutingPeer {
    fn default() -> Self {
	let address = Key::default();
	let peer = "0.0.0.0:0".parse().unwrap();
	Self::new(&address, peer, address)
    }
}


struct RoutingPeerMutable {
    /// Last known contact (note: Kad provides no spoofing protection)
    peer: SocketAddr,
    /// Have received packet from alternate address, to become the main address it has
    /// to be verified by first pinging the peer address (repeatedly) and if it doesn't answer
    /// the alter_peer address can be pinged and put to use if it works
    alter_peer: Option<SocketAddr>,
    /// Last time we successfully got data from peer
    last_seen: Instant,
    /// Last time we tried to contact peer
    last_try: Instant,
}
