mod global_channel;
mod local_channel;

pub use self::global_channel::{GlobalChannel, connect};
pub use self::local_channel::{LocalChannel, PublishSink};


use futures::{Stream, Sink};
use ex_futures::stream::UnsyncCloneable as UnsyncCloneableStream;
use ex_futures::sink::UnsyncCloneable as UnsyncCloneableSink;

use amqpr_codec::Frame;

use std::rc::Rc;

use errors::Error;

type Income = UnsyncCloneableStream<Box<Stream<Item = Frame, Error = Rc<Error>>>>;
type Outcome = UnsyncCloneableSink<Box<Sink<SinkItem = Frame, SinkError = Rc<Error>>>>;
