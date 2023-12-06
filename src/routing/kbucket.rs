use std::sync::Arc;

use itertools::Itertools;

use serde::{Deserialize, Serialize};

use crate::util::ringbuffer::RingBuffer;
use super::constants::*;
use super::peer::*;


/// On-disk format for KBucket
#[derive(Serialize, Deserialize)]
pub struct KBucketDiskV1 {
    peers: Vec<RoutingPeerDiskV1>,
    cache: Vec<RoutingPeerDiskV1>,
}


pub(crate) struct KBucket {
    /// The active route targets in this KBucket.
    pub(crate) peers: RingBuffer<Peer, KAD_K>,
    /// When KBucket is full, place peers temporarly here awaiting a split/drop
    pub(crate) cache: RingBuffer<Peer, K_CACHE_SIZE>,
}


impl KBucket {
    pub fn new() -> KBucket {
	KBucket {
	    peers: RingBuffer::default(),
	    cache: RingBuffer::default(),
	}
    }

    pub fn insert(&mut self, peer: Peer) -> bool {
	if !self.peers.is_full() {
	    self.peers.push(peer);
	    false
	} else {
	    // Bucket is full, but save the node as a replacement
	    // Replacement happens when dead node is the route head.
	    self.cache.push(peer);
	    // Check if we are ripe for a split
	    let failing: usize = self.peers.iter()
		.map(|item| if item.get_errors() > 0 { 1 } else { 0 })
		.sum();
	    // If we have more failing nodes than cache items, we won't be able to
	    // split, but OTOH, the bucket will return to homeostasis after a while
	    // and the failing nodes will be dropped..
	    if failing < self.cache.len() {
		true
	    } else {
		false
	    }
	}
    }

    /// Take peers from cache after a balance
    fn _balance_cache(&mut self) {
	while self.peers.len() < self.peers.capacity() && self.cache.len() > 0 {
	    self.peers.push(self.cache.pop().unwrap());
	}
    }

    /// Partition the RingBuffer by the bit position - see split(..) and divide(..).
    fn _partition<const S: usize>(buf: &RingBuffer<Peer, S>, bits: usize) -> [RingBuffer<Peer, S>; 2] {
	buf.iter()
	    .map(|rpeer| rpeer.clone())
	    .partition(|peer| peer.distance.is_set(bits))
	    .into()
    }

    fn _partitioned_kbucket(&self, bits: usize) -> (KBucket, KBucket) {
	let peers = Self::_partition(&self.peers, bits);
	let cache = Self::_partition(&self.cache, bits);
	let mut buckets: Vec<_> = peers.into_iter()
	    .zip(cache.into_iter())
	    .map(|(peers, cache)| KBucket { peers, cache })
	    .collect();
	// We know that the vec has two items from the partitioning.
	let mut close = buckets.pop().unwrap();
	let mut far   = buckets.pop().unwrap();
	close._balance_cache();
	far._balance_cache();
	(far, close)
    }

    /// Divide bucket into (far, close) pair (not to be confused with split buckets).
    /// This only works on the last (i.e. "home") bucket.
    /// e.g. w/ bits = 2 one containing 001Y and the other 000Y.
    pub fn divide(&self, bits: usize) -> (KBucket, KBucket) {
	self._partitioned_kbucket(bits)
    }

    /// Make split bucket (far, close) pair at `bits` bits (not to be confused by divided bucket).
    /// e.g. w/ bits = 2 into 0011Y and 0010Y
    pub fn split(&self, bits: usize) -> (KBucket, KBucket) {
	self._partitioned_kbucket(bits + 1)
    }

    /// Merge two recently split buckets into one, trying to preserve balance
    pub fn merge(a: &Self, b: &Self) -> KBucket {
	// First, try to include from the main peers, ignoring the ones with errors
	// (controversial - should be include ones with errors?)
	let no_errors = |peer: &&Peer| !peer.has_errors();
	let a_it = a.peers.iter().filter(no_errors);
	let b_it = b.peers.iter().filter(no_errors);
	let mut peers: RingBuffer<Peer, KAD_K> = a_it.interleave(b_it)
	    .map(|pref| pref.clone())
	    .collect();

	if peers.len() < peers.capacity() {
	    // There were not enough healthy nodes, take the ones with errors
	    let only_errors = |peer: &&Peer| peer.has_errors();
	    let a_it = a.peers.iter().filter(only_errors);
	    let b_it = b.peers.iter().filter(only_errors);
	    for peer in a_it.interleave(b_it).map(|pref| pref.clone()) {
		if peers.len() == peers.capacity() {
		    break;
		}
		peers.push(peer);
	    }
	}
	// Also interleave the cache (could obviously go with merge by timestamp,
	// but it might be better to interleave by keyspace).
	let cache = a.cache.iter()
	    .interleave(b.cache.iter())
	    .map(|pref| pref.clone())
	    .collect();
	KBucket {
	    peers,
	    cache,
	}
    }

    pub fn serialize(&self) -> KBucketDiskV1 {
	let peers = self.peers.iter().map(|p| p.serialize()).collect();
	let cache = self.cache.iter().map(|p| p.serialize()).collect();
	KBucketDiskV1 {
	    peers,
	    cache,
	}
    }
}


impl From<KBucketDiskV1> for KBucket {
    fn from(disk: KBucketDiskV1) -> Self {
	let peers = disk.peers.into_iter()
	    .map(|p| Arc::new(p.into()))
	    .collect();
	let cache = disk.cache.into_iter()
	    .map(|p| Arc::new(p.into()))
	    .collect();
	KBucket {
	    peers,
	    cache
	}
    }
}
