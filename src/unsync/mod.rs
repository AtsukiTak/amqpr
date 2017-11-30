mod global_channel;
mod local_channel;

pub use self::global_channel::{GlobalChannel, connect};
pub use self::local_channel::{LocalChannel, PublishSink};


use futures::{Stream, Sink, Future};

use amqpr_codec::Frame;

use std::rc::Rc;

use errors::Error;


pub type AmqpFuture<T> = Future<Item = T, Error = Rc<Error>>;


pub trait Income: Stream<Item = Frame, Error = Rc<Error>> + 'static {}
pub trait Outgo: Sink<SinkItem = Frame, SinkError = Rc<Error>> + 'static {}

pub type BoxedIncome = Box<Stream<Item = Frame, Error = Rc<Error>>>;
pub type BoxedOutgo = Box<Sink<SinkItem = Frame, SinkError = Rc<Error>>>;

impl<S> Income for S
where
    S: Stream<Item = Frame, Error = Rc<Error>> + 'static,
{
}

impl<S> Outgo for S
where
    S: Sink<SinkItem = Frame, SinkError = Rc<Error>> + 'static,
{
}
