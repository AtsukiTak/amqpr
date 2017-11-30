use futures::{Future, Stream, Poll, Async};

use ex_futures::sink::{SinkExt, UnsyncCloneable};
use ex_futures::stream::StreamExt;

use amqpr_api::basic::consume::{start_consume_wait, ConsumeStarted};
use amqpr_api::basic::deliver::{receive_delivered, Delivered};
use amqpr_codec::Frame;

use bytes::Bytes;

use std::rc::Rc;

use unsync::{Income, Outgo, BoxedIncome, LocalChannel};
use errors::Error;


pub use amqpr_api::basic::consume::StartConsumeOption as SubscribeOption;



/// Create a stream of subscribed item from AMQP server.
/// That stream is based on `no_ack` consume so that increase performance.
/// But that cause a decreasing of reliability.
/// If you want reliability rather than performance, you should use `subscribe_stream_ack`
/// function.
pub fn subscribe_stream<In: Income, Out: Outgo>(
    local_ch: LocalChannel<In, Out>,
    queue: String,
    consumer_tag: String,
    no_local: bool,
    exclusive: bool,
) -> (LocalChannel<BoxedIncome, UnsyncCloneable<Out>>,
      SubscribeStream<BoxedIncome, UnsyncCloneable<Out>>) {
    let option = SubscribeOption {
        queue: queue,
        consumer_tag: consumer_tag.clone(),
        is_no_local: no_local,
        is_no_ack: true,
        is_exclusive: exclusive,
        is_no_wait: false,
    };

    let (id, income, outgo) = (local_ch.channel_id, local_ch.income, local_ch.outgo);
    let cloneable_outgo = outgo.unsync_cloneable();

    // Create `SubscribeStream`
    let mut checker = ItemChecker {
        consumer_tag: consumer_tag,
        expect: Expect::ConsumeOk,
    };
    let (subscribe_income, others_income) = income.unsync_fork(move |frame| checker.check(frame));
    let consume_started = start_consume_wait(
        Box::new(subscribe_income) as BoxedIncome,
        cloneable_outgo.clone(),
        id,
        option,
    );
    let sub_stream = SubscribeStream::SendingConsumeMethod(consume_started);

    // Create LocalChannel
    let local_ch = LocalChannel {
        channel_id: id,
        income: Box::new(others_income) as BoxedIncome,
        outgo: cloneable_outgo,
    };

    (local_ch, sub_stream)
}



struct ItemChecker {
    consumer_tag: String,
    expect: Expect,
}

enum Expect {
    ConsumeOk,
    Deliver,
    ContentHeader,
    ContentBody,
}

// {{{ impl of ItemChecker
impl ItemChecker {
    fn check<'a, 'b>(&'a mut self, frame: &'b Frame) -> bool {
        let new_expect = match self.expect {
            Expect::ConsumeOk => {
                if frame
                    .method()
                    .and_then(|c| c.basic())
                    .and_then(|m| m.consume_ok())
                    .map(|f| f.consumer_tag.as_str() == self.consumer_tag.as_str())
                    .unwrap_or(false)
                {
                    Expect::Deliver
                } else {
                    return false;
                }
            }
            Expect::Deliver => {
                if frame
                    .method()
                    .and_then(|c| c.basic())
                    .and_then(|c| c.deliver())
                    .map(|f| f.consumer_tag.as_str() == self.consumer_tag.as_str())
                    .unwrap_or(false)
                {
                    Expect::ContentHeader
                } else {
                    return false;
                }
            }
            Expect::ContentHeader => {
                if frame.content_header().is_some() {
                    Expect::ContentBody
                } else {
                    return false;
                }
            }
            Expect::ContentBody => {
                if frame.content_body().is_some() {
                    Expect::Deliver
                } else {
                    return false;
                }
            }
        };

        self.expect = new_expect;

        true
    }
}
// }}}



/// Stream of subscribed item from AMQP server.
/// This stream is based on `no_ack` consume so that increase performance.
/// But that cause a decreasing of reliability.
/// If you want reliability rather than performance, you should use `subscribe_stream_ack`
/// function.
pub enum SubscribeStream<In: Income, Out: Outgo> {
    SendingConsumeMethod(ConsumeStarted<In, Out>),
    ReceivingDeliverd(Delivered<In>),
}


impl<In: Income, Out: Outgo> Stream for SubscribeStream<In, Out> {
    type Item = Bytes;
    type Error = Rc<Error>;

    fn poll(&mut self) -> Poll<Option<Bytes>, Self::Error> {
        use self::SubscribeStream::*;

        let (bytes_opt, income) = match self {
            &mut SendingConsumeMethod(ref mut fut) => {
                let (income, outgo) = try_ready!(fut.poll());
                drop(outgo);
                (None, income)
            }
            &mut ReceivingDeliverd(ref mut del) => {
                let (bytes, income) = try_ready!(del.poll());
                (Some(bytes), income)
            }
        };

        let del = receive_delivered(income);
        *self = ReceivingDeliverd(del);

        match bytes_opt {
            Some(bytes) => Ok(Async::Ready(Some(bytes))),
            None => self.poll(),
        }
    }
}
