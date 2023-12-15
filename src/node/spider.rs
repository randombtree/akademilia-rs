use std::sync::{
    Arc,
    atomic,
};
use std::cmp::{Ord, Ordering, PartialOrd};

use paste::paste;

use crate::routing::{
    peer::{Peer, },
};

#[derive(Clone)]
pub struct SpiderPeer(Arc<SpiderPeerInner>);

pub struct SpiderPeerInner {
    peer: Peer,
    visited: atomic::AtomicBool,
    finished: atomic::AtomicBool,
    stored: atomic::AtomicBool,
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


macro_rules! atomic_getset {
    ($self_:ident, $name:ident) => {
	paste! {
	    #[allow(unused)]
	    pub fn [<has _ $name>](& $self_) -> bool {
		$self_.0.$name.load(atomic::Ordering::Relaxed)
	    }

	    #[allow(unused)]
	    pub fn [<set _ $name>](& $self_) {
		$self_.0.$name.store(true, atomic::Ordering::Relaxed)
	    }

	    #[allow(unused)]
	    pub fn [<clear _ $name>](& $self_) {
		$self_.0.$name.store(false, atomic::Ordering::Relaxed)
	    }
	}
    }
}


impl SpiderPeer {
    pub fn new(peer: Peer) -> SpiderPeer {
	SpiderPeer(Arc::new(SpiderPeerInner {
	    peer,
	    visited: atomic::AtomicBool::new(false),
	    stored: atomic::AtomicBool::new(false),
	    finished: atomic::AtomicBool::new(false),
	    failed: atomic::AtomicBool::new(false),
	}))
    }

    pub fn peer(&self) -> &Peer { &self.0.peer }

    atomic_getset!{ self, visited }
    atomic_getset!{ self, stored }
    atomic_getset!{ self, finished }
    atomic_getset!{ self, failed }
}
