use futures::{Sink, Future, Poll, StartSend, Async, AsyncSink};

use ex_futures::util::Should;

use amqpr_api::basic::publish::{publish as publish_, Published};

use bytes::Bytes;

use std::rc::Rc;

use super::{Income, Outcome, LocalChannel};
use errors::*;


pub use amqpr_api::basic::publish::PublishOption;



pub fn publish(ch: LocalChannel, bytes: Bytes, option: PublishOption) -> PublishFuture {
    let (ch_id, income, outcome) = (ch.channel_id, ch.income, ch.outcome);
    let published = publish_(outcome, ch_id, bytes, option);
    PublishFuture {
        store: Should::new((ch_id, income)),
        published: published,
    }
}



/// Future which will return `LocalChannel` when complete to publish bytes.
pub struct PublishFuture {
    store: Should<(u16, Income)>,
    published: Published<Outcome>,
}


impl Future for PublishFuture {
    type Item = LocalChannel;
    type Error = Rc<Error>;

    fn poll(&mut self) -> Poll<LocalChannel, Rc<Error>> {
        let outcome = try_ready!(self.published.poll());
        let (ch_id, income) = self.store.take();
        Ok(Async::Ready(LocalChannel {
            channel_id: ch_id,
            income: income,
            outcome: outcome,
        }))
    }
}





pub fn publish_sink(channel: u16, option: PublishOption, sink: Outcome) -> PublishSink {
    PublishSink {
        channel: channel,
        option: option,
        state: PublishState::Waiting(Should::new(sink)),
    }
}


pub struct PublishSink {
    channel: u16,
    option: PublishOption,
    state: PublishState,
}


enum PublishState {
    Processing(Published<Outcome>),
    Waiting(Should<Outcome>),
}


impl Sink for PublishSink {
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
