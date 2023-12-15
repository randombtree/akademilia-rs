/// Misc future helpers
use std::pin::Pin;
use std::future::Future;
use std::task::Context;
use std::task::Poll;

use futures::future::FusedFuture;
/// Placeholder future for use in e.g. select!.
/// Will act as a pending future as long as the actual future is missing.
pub struct PlaceholderFuture<T> {
    actual: Option<Pin<Box<dyn FusedFuture<Output=T>>>>,
}


impl<T: 'static> PlaceholderFuture<T> {
    pub fn new() -> PlaceholderFuture<T> {
	PlaceholderFuture {
	    actual: None,
	}
    }

    pub fn apply(&mut self, fut: Pin<Box<dyn FusedFuture<Output=T>>>) {
	assert!(self.actual.is_none() || self.actual.as_deref().unwrap().is_terminated());
	self.actual = Some(fut);
    }
}


impl<T: 'static> Future for PlaceholderFuture<T> {
    type Output = T;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
	let inner = self.get_mut();
	inner.actual.as_mut()
	    .map(|fut| Future::poll(fut.as_mut(), cx))
	    .or(Some(Poll::Pending))
	    .unwrap()
    }
}


impl<T: 'static> FusedFuture for PlaceholderFuture<T> {
    fn is_terminated(&self) -> bool {
	self.actual.as_ref()
	    .map(|fut| fut.is_terminated())
	    .or(Some(false))
	    .unwrap()
    }
}
