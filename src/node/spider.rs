use std::sync::{
    Arc,
    atomic,
};
use std::cmp::{Ord, Ordering, PartialOrd};


use crate::routing::{
    peer::{Peer, },
};

#[derive(Clone)]
pub struct SpiderPeer(Arc<SpiderPeerInner>);

pub struct SpiderPeerInner {
    peer: Peer,
    visited: atomic::AtomicBool,
    failed: atomic::AtomicBool,
}


impl PartialEq for SpiderPeer {
    fn eq(&self, other: &Self) -> bool {
	(*self.0.peer).eq(&*other.0.peer)
    }
}


impl Eq for SpiderPeer {}


impl PartialOrd for SpiderPeer {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
	(*self.0.peer).partial_cmp(&*other.0.peer)
    }
}


impl Ord for SpiderPeer {
    fn cmp(&self, other: &Self) -> Ordering {
	self.partial_cmp(other).unwrap()
    }
}


impl SpiderPeer {
    pub fn new(peer: Peer) -> SpiderPeer {
	SpiderPeer(Arc::new(SpiderPeerInner {
	    peer,
	    visited: atomic::AtomicBool::new(false),
	    failed: atomic::AtomicBool::new(false),
	}))
    }

    pub fn peer(&self) -> &Peer { &self.0.peer }

    pub fn set_visited(&self) {
	self.0.visited.store(true, atomic::Ordering::Relaxed);
    }

    pub fn visited(&self) -> bool {
	self.0.visited.load(atomic::Ordering::Relaxed)
    }

    pub fn set_failed(&self) {
	self.0.failed.store(true, atomic::Ordering::Relaxed);
    }

    pub fn failed(&self) -> bool {
	self.0.failed.load(atomic::Ordering::Relaxed)
    }
}
