/// Simple ringbuffer optimized for working full-sized with rotation.
/// Rotating a full ring-buffer only updates two pointers.
use std::ops::{
    Index,
    IndexMut,
};
use std::alloc:: {
    self,
    Layout,
};
use std::iter::{
    Extend,
    FromIterator,
    Iterator,
};
use std::ptr::NonNull;

pub struct RingBuffer<T, const S: usize>
where
    T: Sized,
{
    ptr: NonNull<T>,
    head: usize,
    tail: usize,
    length: usize,
}


impl<T, const S: usize> Default for RingBuffer<T,S>
where
    T: Sized,
{
    fn default() -> Self {
	// Unsafe: Allocate ring-buffer, panic on failure. Free'd in drop.
	let ptr = unsafe { std::alloc::alloc(Self::LAYOUT) };
        let ptr = match NonNull::new(ptr as *mut T) {
            Some(p) => p,
            None => alloc::handle_alloc_error(Self::LAYOUT),
        };

	RingBuffer {
	    ptr,
	    head: 0,
	    tail: 0,
	    length: 0,
	}
    }
}


impl<T, const S: usize> Drop for RingBuffer<T,S>
where
    T: Sized,
{
    fn drop(&mut self) {
	// Call drop on contents
	for i in 0..self.length {
	    let offset = (self.head + i) % S;
	    // Unsafe: offset is % S, and item is "alive" as index is "between" head and tail.
	    unsafe {
		std::ptr::drop_in_place(self.ptr.as_ptr().add(offset));
	    }
	}

	// Unsafe: ptr is guaranteed to be non-null, contained items dropped above ^
	unsafe {
	    alloc::dealloc(self.ptr.as_ptr() as *mut u8, Self::LAYOUT);
	}
    }
}


impl<T, const S: usize> RingBuffer<T,S>
where
    T: Sized,
{
    const LAYOUT: Layout = match Layout::array::<T>(S) {
	Ok(l) => l,
	Err(_) => panic!("No layout for type"),
    };
    pub fn is_full(&self) -> bool { self.length == S }
    pub fn is_empty(&self) -> bool { self.length == 0 }
    pub fn len(&self) -> usize { self.length }
    pub fn capacity(&self) -> usize { S }

    /// Push an item to the ringbuffers tail
    /// Returns head item if ring is full
    pub fn push(&mut self, item: T) -> Option<T> {
	let ret = {
	    if self.is_full() {
		self.pop_head()
	    } else {
		None
	    }
	};

	self.tail = (self.tail + 1) % S;
	if self.length == 0 {
	    self.head = self.tail;
	}
	self.length += 1;
	// Unsafe: self.tail is % S -> Safe
	unsafe {
	    std::ptr::write(self.ptr.as_ptr().add(self.tail), item);
	}
	ret
    }

    /// Pop an item from the tail (i.e. the most recently pushed)
    pub fn pop(&mut self) -> Option<T> {
	(!self.is_empty()).then(|| {
	    let tail = self.tail;
	    self.tail = (self.tail + S - 1) % 8;
	    self.length -= 1;
	    // Unsafe: self.tail is safe
	    unsafe { std::ptr::read(self.ptr.as_ptr().add(tail)) }
	})
    }

    /// Pop an item from the ringbuffers head
    pub fn pop_head(&mut self) -> Option<T> {
	(self.length > 0).then(|| {
	    let head  = self.head;
	    self.head = (self.head + 1) % S;
	    self.length -= 1;
	    // Unsafe: head is (from self.head) % S, length > 0 -> OK
	    unsafe { std::ptr::read(self.ptr.as_ptr().add(head)) }
	})
    }

    /// Swap item on head
    pub fn swap_head(&mut self, item: &mut T) {
	assert!(!self.is_empty());
	// Unsafe: self.head is % S
	unsafe {
	    std::mem::swap(item, self.ptr.as_ptr().add(self.head).as_mut().unwrap());
	}
    }

    /// Get n:th item in buffer (0 = head)
    pub fn get(&self, index: usize) -> Option<&T> {
	(index < self.length).then(|| {
	    // Unsafe: Indexing with % S -> Safe
	    unsafe {
		self.ptr.as_ptr().add((self.head + index) % S)
		    .as_ref().unwrap()
	    }
	})
    }

    /// Get n:th item in buffer, mutable reference
    pub fn get_mut(&mut self, index: usize) -> Option<&mut T> {
	(index < self.length).then(|| {
	    // Unsafe: Indexing with % S -> Safe
	    unsafe {
		self.ptr.as_ptr().add((self.head + index) % S)
		    .as_mut().unwrap()
	    }
	})
    }

    /// Place head item to the tail
    pub fn rotate_head(&mut self) {
	if self.is_full() {
	    // When full, we can just switch the pointers
	    self.head = (self.head + 1) % S;
	    self.tail = (self.tail + 1) % S;
	} else if self.head == self.tail {
	    // pass!
	} else if !self.is_empty() {
	    // Need to move the head item to the tail
	    // swap:in ensures that item drop method isn't run
	    self.tail = (self.tail + 1) % S;
	    // Unsafe: head and tail are both %S -> Safe
	    let (tail, head) = unsafe {
		(self.ptr.as_ptr().add(self.tail).as_mut().unwrap(),
		 self.ptr.as_ptr().add(self.head).as_mut().unwrap())
	    };
	    std::mem::swap(tail, head);
	    self.head = (self.head + 1) % S;
	}
    }

    /// Get reference to head item
    pub fn get_head(&self) -> &T {
	self.get(0).expect("Buffer underflow")
    }

    /// Get mutable reference to head item
    pub fn get_head_mut(&mut self) -> &mut T {
	self.get_mut(0).expect("Buffer underflow")
    }

    pub fn iter<'a>(&'a self) -> RingIterator<'a, T, S> {
	RingIterator::new(self)
    }
}


impl <T, const S: usize> Index<usize> for RingBuffer<T,S>
where
    T: Sized,
{
    type Output = T;
    fn index(&self, index: usize) -> &Self::Output {
	self.get(index).expect("Index out of bounds")
    }
}


impl <T, const S: usize> IndexMut<usize> for RingBuffer<T,S>
where
    T: Sized,
{
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
	self.get_mut(index).expect("Index out of bounds")
    }
}


pub struct RingIterator<'a, T, const S: usize> {
    pos: usize,
    ring: &'a RingBuffer<T, S>,
}


impl<'a, T, const S: usize> RingIterator<'a, T, S> {
    pub fn new(ring: &'a RingBuffer<T, S>) -> RingIterator<'a, T, S> {
	RingIterator {
	    pos: 0,
	    ring,
	}
    }
}


impl<'a, T, const S: usize> Iterator for RingIterator<'a, T, S> {
    type Item = &'a T;
    fn next(&mut self) -> Option<Self::Item> {
	self.ring.get(self.pos)
	    .and_then(|item| {
		self.pos += 1;
		Some(item)
	    })
    }
}


/// Create ring buffer from the iterator head items
impl <T, const S: usize> FromIterator<T> for RingBuffer<T, S> {
    fn from_iter<I>(iter: I) -> Self
    where I: IntoIterator<Item = T> {
	let mut rb = RingBuffer::default();
	for item in iter {
	    if rb.length >= rb.capacity() {
		break;
	    }
	    rb.push(item);
	}
	rb
    }
}


/// Extend RingBuffer from iterator.
/// NB: It may push out old items if it overflows
impl <T, const S: usize> Extend<T> for RingBuffer<T, S> {
    fn extend<I>(&mut self, iter: I)
    where I: IntoIterator<Item = T> {
	for item in iter {
	    self.push(item);
	}
    }
}

unsafe impl <T: Send, const S: usize> Send for RingBuffer<T, S> {}
unsafe impl <T: Sync, const S: usize> Sync for RingBuffer<T, S> {}

#[cfg(test)]
mod test {
    use super::*;
    const SZ: usize = 5;
    type UsizeRing = RingBuffer<usize, SZ>;
    fn get_full_buffer() -> UsizeRing {
	let mut rb = RingBuffer::default();
	for i in 0..SZ {
	    rb.push(i);
	}
	rb
    }

    fn validate_buffer(rb: &UsizeRing) {
	for (ndx, i) in rb.iter().enumerate() {
	    assert!(ndx == *i, "Mismatch at index {}: {}", ndx, i);
	}
    }

    #[test]
    fn test_ring_buffer_push_pop() {
	let mut rb = RingBuffer::<usize, SZ>::default();
	assert!(rb.capacity() == SZ);
	// Try different fill grades, re-use buffer to test out the wrap-over
	for count in 1..SZ {
	    for i in 0..count {
		rb.push(i);
	    }
	    assert!(rb.len() == count);
	    for i in 0..count {
		assert!(count - 1 == *rb.get(rb.len() - 1).unwrap());
		assert!(i == *rb.get(0).unwrap());
		assert!(i == rb.pop_head().unwrap());
	    }
	    assert!(rb.is_empty());
	}
	// Test that push pops the previous head
	for i in 0..SZ {
	    rb.push(i);
	}
	assert!(Some(0) == rb.push(SZ));
    }

    #[test]
    fn test_ring_buffer_rotate() {
	let mut rb = get_full_buffer();

	// Rotate full
	for i in 0..SZ {
	    rb.rotate_head();
	    assert!(*rb.get_head() == (i + 1) % SZ);
	}

	assert!(0 == rb.pop_head().unwrap());
	// Without full buffer
	for i in 1..(SZ - 1) {
	    rb.rotate_head();
	    assert!(*rb.get_head() == i + 1);
	}
	rb.rotate_head();
	assert!(*rb.get_head() == 1);
	rb.push(0);
	// Order now 1 2 3 4 0, fix that
	for _ in 1..SZ {
	    rb.rotate_head();
	}
	// And now should be in order
	for i in 0..SZ {
	    assert!(i == rb.pop_head().unwrap());
	}
    }

    #[test]
    fn test_ring_buffer_mut() {
	let mut rb = get_full_buffer();

	for i in 0..SZ {
	    assert!(rb[i] == i);
	    rb[i] += 1;
	    assert!(rb[i] == i + 1);
	}

	for i in 0..SZ {
	    assert!(i + 1 == rb.pop_head().unwrap());
	}
    }

    #[test]
    fn test_ring_buffer_misc() {
	let mut rb = get_full_buffer();
	*rb.get_head_mut() = 1;
	assert!(rb[0] == 1, "get_head_mut");

	let mut n = 0;
	rb.swap_head(&mut n);
	assert!(n == 1);
	for i in 0..SZ {
	    assert!(rb[i] == i);
	}
    }

    #[test]
    fn test_ring_buffer_drop() {
	// Use Rc for it's ability to count the references, which decrease after a drop().
	// Also get nice segfault if drop() is called on uninitialized values :)
	use std::rc::Rc;
	let item1 = Rc::new(1);
	let item2 = Rc::new(2);
	let mut rb = RingBuffer::<Rc<usize>, SZ>::default();
	rb.push(item1.clone());
	rb.push(item2.clone());
	// Rotate should not call drop
	rb.rotate_head(); // => Order: 2 1
	assert!(Rc::strong_count(&item1) == 2);
	assert!(Rc::strong_count(&item2) == 2);
	let item3 = Rc::new(3);
	let mut item3_1 = item3.clone();
	rb.swap_head(&mut item3_1); // => Order: 3 1

	assert!(Rc::strong_count(&item3) == 2);
	assert!(Rc::strong_count(&item2) == 2);
	assert!(*item3_1 == 2);
	drop(item3_1);
	assert!(Rc::strong_count(&item2) == 1);
	drop(rb);
	// Drop() must have been run on the remaining items (3 1)
	assert!(Rc::strong_count(&item1) == 1);
	assert!(Rc::strong_count(&item3) == 1);
    }

    #[test]
    fn test_ring_buffer_iter() {
	let rb = get_full_buffer();
	for (ndx, item) in rb.iter().enumerate() {
	    assert!(ndx == *item);
	}
    }

    #[test]
    fn test_ring_buffer_from_iter_sized() {
	let rb: UsizeRing = (0..SZ).collect();
	assert!(SZ == rb.len());
	validate_buffer(&rb);
    }

    #[test]
    fn test_ring_buffer_from_iter_undersized() {
	const SZ_PARTIAL: usize = SZ - 1;
	let rb: UsizeRing = (0..SZ_PARTIAL).collect();
	assert!(SZ_PARTIAL == rb.len());
	validate_buffer(&rb);
    }

    #[test]
    fn test_ring_buffer_from_iter_oversized() {
	const SZ_LARGE: usize = SZ + 1;
	let rb: UsizeRing = (0..SZ_LARGE).collect();
	assert!(SZ == rb.len());
	validate_buffer(&rb);
    }

    #[test]
    fn test_ring_buffer_extend() {
	let mut rb: UsizeRing = RingBuffer::default();
	rb.extend(0..SZ);
	validate_buffer(&rb);
    }
}
