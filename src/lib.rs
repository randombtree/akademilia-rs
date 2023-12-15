#![feature(const_alloc_layout)]

#[cfg(all(feature = "async-std", feature = "tokio"))]
compile_error!("Choose either \"async-std\" or \"tokio\" feature, not both!");

#[cfg(not(any(feature = "async-std", feature = "tokio")))]
compile_error!("Choose either \"async-std\" or \"tokio\" feature");

pub mod routing;
pub mod node;
pub mod util {
    pub mod ringbuffer;
    pub(crate) mod futures;
}
