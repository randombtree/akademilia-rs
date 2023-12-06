/// Routing table
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Weak};

#[cfg(test)]
use std::slice::Iter;

use log::trace;

use serde::{Deserialize, Serialize};

use super::key::*;
use super::peer::*;
use super::kbucket::{KBucket, KBucketDiskV1};


/// On-disk format for RoutingTable
#[derive(Serialize, Deserialize)]
pub(crate) struct RTDiskV1 {
    node: Key,
    buckets: Vec<KBucketDiskV1>,
}


/// Kademlia routing table
pub struct RoutingTable {
    node: PeerAddress,
    buckets: Vec<KBucket>,
    peers: HashMap<PeerAddress, Weak<RoutingPeer>>,
}


impl RoutingTable {
    pub fn new(node: PeerAddress) -> RoutingTable {
	let buckets = vec![KBucket::new()];
	let peers = HashMap::new();
	RoutingTable {
	    node,
	    buckets,
	    peers,
	}
    }

    pub fn node(&self) -> PeerAddress {
	self.node.clone()
    }

    /// Get peer for incoming source and with kad address.
    pub fn get_peer(&mut self, saddr: SocketAddr, address: Key) -> Peer {
	self.peers.get(&address).and_then(|peer| {
	    // TODO: Update saddr!
	    // Needs to check that the original address isn't working anymore with a ping first!
	    peer.upgrade()
		.and_then(|peer| {
		    trace!("Existing peer {}", saddr);
		    Some(peer)
		})
	}).or_else(|| {
	    trace!("New peer {}", saddr);
	    let peer = Arc::new(RoutingPeer::new(&self.node, saddr, address));
	    self.peers.insert(peer.address.clone(), Arc::downgrade(&peer));
	    self.insert(peer.clone());
	    Some(peer)
	}).unwrap()
    }

    fn insert(&mut self, peer: Peer) {
	/*
	 * Original KAD paper uses a tree-layout for buckets, however,
	 * by "cheating" a bit we get to use a much faster Vec.
	 * The split bucket allows the "relaxed" branching mentioned in Fig. 5,
	 * allowing us to know k-contacts around the local keyspace.
	 * Bucket layout (n buckets):
	 * Vec: |0| |1| |3| .. |n - 3|  |n - 2|  |n - 1|
	 *      \ foreign /    \ split bucket /   local
	 * Bits: [0, n - 4]        n - 3         [n - 2,..]
	 */
	trace!("Insert {}", peer.distance);
	let len = self.buckets.len();
	let bits = usize::from(peer.distance.bits());
	let bucket = {
	    if len == 1 {
		0
	    } else if bits < len - 3 {
		// Foreign bucket
		bits
	    } else if bits >= len - 2 {
		// Local bucket, our k-closest
		len - 1
	    } else if bits == len - 3 {
		// Split bucket
		// Have a distance ..01xY, split in the far and close bucket
		// i.e. 011Y is far, and 010Y is close
		let x_bit: usize = (bits + 1).into();
		if peer.distance.is_set(x_bit) {
		    // x == 1
		    len - 3
		} else {
		    // x == 0
		    len - 2
		}
	    } else {
		panic!();
	    }
	};
	let divide = self.buckets[bucket].insert(peer);
	// Only ever divide due to local bucket filling up
	if divide && bucket == len - 1 {
	    trace!("Need to divide");
	    let home_bits = {
		if len == 1 {
		    0
		} else {
		    len - 2
		}
	    };
	    let (far, home) = self.buckets.pop().unwrap()
		.divide(home_bits);
	    if len > 1 {
		// Need to merge the old split bucket to one
		let (a, b) = (self.buckets.pop().unwrap(), self.buckets.pop().unwrap());
		self.buckets.push(KBucket::merge(&a, &b));
	    }
	    // And still need to make two new split buckets
	    let (far, close) =  far.split(home_bits);
	    self.buckets.push(far);
	    self.buckets.push(close);
	    self.buckets.push(home);
	}

	if divide {
	    // Some peer might have been dropped, refresh hashmap
	    self.peers.retain(|_k, weak| weak.strong_count() > 0);
	}
    }

    pub(crate) fn serialize(&self) -> RTDiskV1 {
	let buckets = self.buckets.iter().map(|kb| kb.serialize()).collect();
	let node = *self.node.clone();
	RTDiskV1 {
	    buckets,
	    node,
	}
    }

    #[cfg(test)]
    pub(crate) fn iter_buckets<'a>(&'a self) -> Iter<'a, KBucket> {
	self.buckets.iter()
    }
}


impl From <RTDiskV1> for RoutingTable {
    fn from(disk: RTDiskV1) -> Self {
	let node = Arc::new(disk.node);
	let mut peers = HashMap::new();
	let buckets = disk.buckets.into_iter().map(|dbucket| {
	    let kbucket: KBucket = dbucket.into();
	    // Need to add the peers to the hashmap
	    for peer in kbucket.peers.iter().chain(kbucket.cache.iter()) {
		peers.insert(peer.address.clone(), Arc::downgrade(peer));
	    }
	    kbucket
	}).collect();
	RoutingTable {
	    node,
	    buckets,
	    peers,
	}
    }
}
