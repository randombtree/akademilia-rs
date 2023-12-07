/// Kademlia replication constant
pub const KAD_K: usize = 20;

/// KBucket LRU cache size, 20%
pub const K_CACHE_SIZE: usize = KAD_K / 5;


/// Kademlia keys size (256 vs. original 160 bits)
pub const KAD_BITS: usize = 256;
pub const KAD_BYTES: usize = KAD_BITS / 8;

/// Kick peer from KBucket after this many errors
pub const MAX_ERRORS: usize = 5;

/// Maximum time to wait for node reply
pub const MAX_RTT_MS: u64 = 1500;
