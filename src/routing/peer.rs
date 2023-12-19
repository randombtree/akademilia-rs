use std::net::SocketAddr;
use std::time::{
    Duration,
    Instant,
    SystemTime,
    UNIX_EPOCH,
};
use std::sync::{Arc, Mutex};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::fmt::{Display, Formatter, Error as FmtError};

use serde::{Deserialize, Serialize};

use super::key::*;


pub type Peer = Arc<RoutingPeer>;
pub type PeerAddress = Arc<Key>;


/// On-disk format for RoutingPeer
#[derive(Serialize, Deserialize)]
pub struct RoutingPeerDiskV1 {
    address: Key,
    distance: Distance,
    peer: SocketAddr,
    alter_peer: Option<SocketAddr>,
    last_seen: u64,
    last_try: u64,
    errors: u32,
}


fn to_unix_time(time: SystemTime) -> u64 {
    let duration = time.duration_since(UNIX_EPOCH).expect("Can't handle time-travel");
    duration.as_secs() + {
	if duration.subsec_nanos() >= 500_000_000 {
	    1
	} else {
	    0
	}
    }
}

fn from_unix_time(secs: u64) -> SystemTime {
    UNIX_EPOCH + Duration::new(secs, 0)
}


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

    pub fn address(&self) -> PeerAddress {
	self.address.clone()
    }

    pub fn peer(&self) -> SocketAddr {
	self.mutable.lock().and_then(|m| Ok(m.peer)).unwrap()
    }

    pub fn alter_peer(&self) -> Option<SocketAddr> {
	self.mutable.lock().and_then(|m| Ok(m.alter_peer)).unwrap()
    }

    pub fn last_seen(&self) -> Instant {
	self.mutable.lock().and_then(|m| Ok(m.last_seen)).unwrap()
    }

    pub fn last_try(&self) -> Instant {
	self.mutable.lock().and_then(|m| Ok(m.last_try)).unwrap()
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

    /// Return a serializable struct
    pub fn serialize(&self) -> RoutingPeerDiskV1 {
	let address = *self.address.clone();
	let distance = self.distance.clone();
	let errors = self.get_errors();
	// Need to convert between monotonic clock and system clock
	let now_monotonic = Instant::now();
	let now_system = SystemTime::now();
	let (last_seen, last_try, peer, alter_peer) =
	    self.mutable.lock().and_then(|mutable| {
		Ok((now_monotonic.duration_since(mutable.last_seen),
		    now_monotonic.duration_since(mutable.last_try),
		    mutable.peer.clone(),
		    mutable.alter_peer.clone(),
		))
	    }).expect("Memory corruption?");

	// Covert to seconds from epoch
	let last_seen = to_unix_time(now_system - last_seen);
	let last_try  = to_unix_time(now_system - last_try);

	RoutingPeerDiskV1 {
	    address,
	    distance,
	    peer,
	    alter_peer,
	    errors,
	    last_seen,
	    last_try,
	}
    }
}


impl From<RoutingPeerDiskV1> for RoutingPeer {
    fn from(disk: RoutingPeerDiskV1) -> Self {
	let now_monotonic = Instant::now();
	let now_system = SystemTime::now();
	// Convert unix time to monotonic time
	// Can't help if system time has gone backwards, just reset time to "now"
	let last_seen = now_monotonic - now_system.duration_since(from_unix_time(disk.last_seen))
	    .unwrap_or(Duration::new(0,0));
	let last_try  = now_monotonic - now_system.duration_since(from_unix_time(disk.last_try))
	    .unwrap_or(Duration::new(0,0));

	let mutable = RoutingPeerMutable {
	    peer: disk.peer,
	    alter_peer: disk.alter_peer,
	    last_seen,
	    last_try,
	};

	let address = Arc::new(disk.address);
	RoutingPeer {
	    address,
	    distance: disk.distance,
	    errors: AtomicU32::new(disk.errors),
	    mutable: Mutex::new(mutable),
	}
    }
}


impl Default for RoutingPeer {
    fn default() -> Self {
	let address = Key::default();
	let peer = "0.0.0.0:0".parse().unwrap();
	Self::new(&address, peer, address)
    }
}


impl PartialEq for RoutingPeer {
    fn eq(&self, other: &RoutingPeer) -> bool {
	self.address == other.address &&
	    self.distance == other.distance
    }
}


impl Eq for RoutingPeer {}


impl PartialOrd for RoutingPeer {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
	// Reverse ordering here:
	// The distance is ordered by matching bits,
	// i.e. the furthest distance is first,
	// But with peers we usually want the closes nodes first :)
	other.distance.partial_cmp(&self.distance)
    }
}


impl Ord for RoutingPeer {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
	self.partial_cmp(other).unwrap()
    }
}


impl Display for RoutingPeer {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), FmtError> {
	// Display distance and last known address
	write!(f, "{} {}",
	       self.distance,
	       self.mutable.lock().map(|m| m.peer).unwrap())
    }
}


#[derive(PartialEq)]
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


#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn test_peer_serialize() {
	let node_key = Key::from_fn(|_| 0xff);

	let peer = RoutingPeer::new(&node_key,
				    "1.2.3.4:1234".parse().unwrap(),
				    Key::from_fn(|_| 0));
	peer.inc_errors();
	peer.update_try();
	peer.update_seen();
	let serialized = peer.serialize();
	println!("{} {}", serialized.last_seen, serialized.last_try);
	let peer2 = serialized.into();

	assert!(peer == peer2);
	// ^ That only checks for the key, but we want to make sure stats etc. are saved:
	let now_monotonic = Instant::now();
	let now_system = SystemTime::now();

	macro_rules! to_unix_time {
	    ($time:expr) => {
		to_unix_time(now_system - now_monotonic.duration_since($time))
	    }
	}
	// Must test at seconds granularity (i.e. as we save).
	assert!(to_unix_time!(peer.last_seen()) == to_unix_time!(peer2.last_seen()));
	assert!(to_unix_time!(peer.last_try()) == to_unix_time!(peer2.last_try()));
	assert!(peer.get_errors() == peer2.get_errors());
	assert!(peer.peer() == peer2.peer());
	assert!(peer.alter_peer() == peer2.alter_peer());
    }
}
