pub mod rtable;
pub mod constants;
pub mod key;
pub mod peer;
mod kbucket;

#[cfg(test)]
mod test {
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::{Arc, Weak};
    use std::collections::BTreeMap;
    use std::time::{Duration, Instant};
    use std::thread;

    use rand::{
	SeedableRng,
	rngs::SmallRng,
    };
    use test_log::test;
    use log::{debug, trace};

    use super::rtable::*;
    use super::peer::*;
    use super::key::*;
    use super::constants::*;

    /// Simple rt test; just test the basics
    #[test]
    fn test_create_rt() {
	// Don't need crypto rng for tests..
	let mut rng = SmallRng::from_entropy();
	let node = Arc::new(Key::from_random(&mut rng));
	let mut rt = RoutingTable::new(node.clone());
	// Fill with KAD_K peers,
	let mut peers = Vec::new();

	// Orders the peers in nodekey descending order, as placed in the kbuckets
	let peer_cmp = |a: &Peer, b: &Peer| a.distance.cmp(&b.distance);
	macro_rules! dump_buckets {
	    () => {
		for (bnum, bucket) in rt.iter_buckets().enumerate() {
		    trace!("Bucket {}", bnum);
		    let mut bucket_peers: Vec<_> = bucket.peers.iter()
			.map(|pref| pref.clone())
			.collect();
		    bucket_peers.sort_by(peer_cmp);
		    for p in bucket_peers {
			trace!("{}", p.distance);
		    }
		}
	    }
	}

	macro_rules! add_nodes {
	    () => {
		for _ in 0..KAD_K {
		    let peer_address = Key::from_random(&mut rng);
		    let addr: SocketAddr = SocketAddr::new(
			IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
			1234);
		    peers.push(rt.get_peer(addr, peer_address));
		    dump_buckets!();
		}
	    }
	}
	add_nodes!();
	assert!(1 == rt.iter_buckets().count());
	add_nodes!();
	trace!("Buckets: {}", rt.iter_buckets().count());
	assert!(3 <= rt.iter_buckets().count());


	peers.sort_by(peer_cmp);
	trace!("Check peers:");
	for p in peers.iter() {
	    trace!("{}", p.distance);
	}
	trace!("-----------");

	let mut peer_iter = peers.iter();
	for (bnum, bucket) in rt.iter_buckets().enumerate() {
	    let mut bucket_peers: Vec<_> = bucket.peers.iter()
		.map(|pref| pref.clone())
		.collect();
	    bucket_peers.sort_by(peer_cmp);
	    trace!("Bucket {} peers:", bnum);
	    for p in bucket_peers.iter() {
		trace!("{}", p.distance);
	    }

	    for peer in bucket_peers {
		let check_peer = peer_iter.next().unwrap();
		assert!(check_peer.address == peer.address, "Mismatch in bucket {} {}, expect {}", bnum, peer.distance, check_peer.distance);
	    }
	}
    }


    /// Stress test RoutingTable for a time-limited duration
    fn do_stress_test(number: usize, duration: Duration) {
	// Don't need crypto rng for tests..
	let mut rng = SmallRng::from_entropy();
	// Cheat a bit with the node, now Distance and key order are the same - which we'll utilize in the check-map
	let node = Arc::new(Key::from_fn(|_| 0xff));
	let mut rt = RoutingTable::new(node.clone());
	let mut truth: BTreeMap<PeerAddress, Weak<RoutingPeer>> = BTreeMap::new();
	let start = Instant::now();
	let mut now = start.clone();
	let mut pass = 0;
	while now.duration_since(start) < duration {
	    debug!("({}) Stress pass {} start..", number, pass);
	    for _ in 0..1000 {
		let peer_address = Key::from_random(&mut rng);
		let addr: SocketAddr = SocketAddr::new(
		    IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
		    1234);

		let peer = rt.get_peer(addr, peer_address);
		let key = peer.address.clone();
		let weak = Arc::downgrade(&peer);
		// There is a slight, tiny, tiny chance the random address isn't unique
		// but in that case we will either replace the dead reference with an new one, or
		// then we insert the same peer..
		truth.insert(key, weak);
	    }
	    debug!("({}) Stress pass {} end", number, pass);

	    let mut bucket_iter = rt.iter_buckets().enumerate()
		.flat_map(|(n, bucket)| {
		    // Do some sanity checks here as well
		    assert!(bucket.cache.is_empty() || bucket.peers.is_full(),
			    "Cache on bucket {} contains items while peer list isn't full",
			    n);
		    let mut peers: Vec<_> = bucket.peers.iter()
			.chain(bucket.cache.iter())
			.collect();
		    peers.sort_by(|a: &&Peer, b: &&Peer| {
			a.distance.cmp(&b.distance)
		    });
		    peers
		});
	    // Clean up btree and check bucket order at the same time
	    truth.retain(|_addr, weak| {
		match weak.upgrade() {
		    Some(peer) => {
			let bucket_peer = bucket_iter.next().unwrap();
			trace!("{} {}", peer.distance, bucket_peer.distance);
			assert!(peer.address == bucket_peer.address,
				"{} Mismatch: Expect {}, got {}",
				number, peer.distance, bucket_peer.distance);
			true
		    },
		    _ => false
		}
	    });
	    now = Instant::now();
	    pass += 1;
	}
    }

    /// Run stress test with 1000's of nodes added (to simulate normal usage)
    #[test]
    fn test_rtable_stress() {
	let count = thread::available_parallelism().unwrap().get();
	let threads: Vec<_> = (0..count).map(|n| {
	    let builder = thread::Builder::new()
		.name(format!("rtable_stress_thread {}", n).into());
	    builder.spawn(move || do_stress_test(n, Duration::new(2, 0))).unwrap()
	}).collect();

	debug!("Waiting for threads to finish");
	for handle in threads {
	    handle.join().unwrap();
	}
    }
}

