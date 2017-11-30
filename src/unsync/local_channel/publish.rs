use futures::{Sink, Future, Poll, StartSend, Async, AsyncSink};

use ex_futures::util::Should;

use amqpr_api::basic::publish::{publish as publish_, Published};

use bytes::Bytes;

use std::rc::Rc;

use super::{Income, Outgo, LocalChannel};
use errors::*;


pub use amqpr_api::basic::publish::PublishOption;



pub fn publish<In: Income, Out: Outgo>(
    ch: LocalChannel<In, Out>,
    bytes: Bytes,
    option: PublishOption,
) -> PublishFuture<In, Out> {
    let (ch_id, income, outgo) = (ch.channel_id, ch.income, ch.outgo);
    let published = publish_(outgo, ch_id, bytes, option);
    PublishFuture {
        store: Should::new((ch_id, income)),
        published: published,
    }
}



/// Future which will return `LocalChannel` when complete to publish bytes.
pub struct PublishFuture<In: Income, Out: Outgo> {
    store: Should<(u16, In)>,
    published: Published<Out>,
}


impl<In: Income, Out: Outgo> Future for PublishFuture<In, Out> {
    type Item = LocalChannel<In, Out>;
    type Error = Rc<Error>;

    fn poll(&mut self) -> Poll<LocalChannel<In, Out>, Rc<Error>> {
        let outgo = try_ready!(self.published.poll());
        let (ch_id, income) = self.store.take();
        Ok(Async::Ready(LocalChannel {
            channel_id: ch_id,
            income: income,
            outgo: outgo,
        }))
    }
}




pub fn publish_sink<Out: Outgo>(
    channel: u16,
    option: PublishOption,
    sink: Out,
) -> PublishSink<Out> {
    PublishSink {
        channel: channel,
        option: option,
        state: PublishState::Waiting(Should::new(sink)),
    }
}


/// A outbound endpoint to publish data.
pub struct PublishSink<Out: Outgo> {
    channel: u16,
    option: PublishOption,
    state: PublishState<Out>,
}


enum PublishState<Out: Outgo> {
    Processing(Published<Out>),
    Waiting(Should<Out>),
}


impl<Out: Outgo> Sink for PublishSink<Out> {
    type SinkItem = Bytes;
    type SinkError = Rc<Error>;

    fn start_send(&mut self, bytes: Bytes) -> StartSend<Bytes, Self::SinkError> {

        if let Async::NotReady = self.poll_complete()? {
            return Ok(AsyncSink::NotReady(bytes));
        }

        use self::PublishState::*;
        self.state = match &mut self.state {
            &mut Processing(ref mut _published) => unreachable!(),
            &mut Waiting(ref mut sink) => {
                let sink = sink.take();
                let published = publish_(sink, self.channel, bytes, self.option.clone());
                Processing(published)
            }
        };

        Ok(AsyncSink::Ready)
    }


    fn poll_complete(&mut self) -> Poll<(), Self::SinkError> {
        use self::PublishState::*;

        self.state = match &mut self.state {
            &mut Processing(ref mut processing) => {
                let sink = try_ready!(processing.poll());
                Waiting(Should::new(sink))
            }
            &mut Waiting(_) => return Ok(Async::Ready(())),
        };

        self.poll_complete()
    }
}
