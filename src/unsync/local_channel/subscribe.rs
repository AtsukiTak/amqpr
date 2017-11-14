use futures::{Future, Stream, Poll, Async};

use amqpr_api::basic::consume::{start_consume_wait, ConsumeStarted};
use amqpr_api::basic::deliver::{receive_delivered, Delivered};

use bytes::Bytes;

use std::rc::Rc;

use super::{Income, Outcome};
use errors::Error;


pub use amqpr_api::basic::consume::StartConsumeOption as SubscribeOption;



/// Create a stream of subscribed item from AMQP server.
/// That stream is based on `no_ack` consume so that increase performance.
/// But that cause a decreasing of reliability.
/// If you want reliability rather than performance, you should use `subscribe_stream_ack`
/// function.
pub fn subscribe_stream(
    channel: u16,
    queue: String,
    consumer_tag: String,
    no_local: bool,
    exclusive: bool,
    income: Income,
    outcome: Outcome,
) -> SubscribeStream {
    let option = SubscribeOption {
        queue: queue,
        consumer_tag: consumer_tag,
        is_no_local: no_local,
        is_no_ack: true,
        is_exclusive: exclusive,
        is_no_wait: false,
    };
    let consume_started = start_consume_wait(income, outcome, channel, option);
    SubscribeStream::SendingConsumeMethod(consume_started)
}


/// Stream of subscribed item from AMQP server.
/// This stream is based on `no_ack` consume so that increase performance.
/// But that cause a decreasing of reliability.
/// If you want reliability rather than performance, you should use `subscribe_stream_ack`
/// function.
pub enum SubscribeStream {
    SendingConsumeMethod(ConsumeStarted<Income, Outcome>),
    ReceivingDeliverd(Delivered<Income>),
}


impl Stream for SubscribeStream {
    type Item = Bytes;
    type Error = Rc<Error>;

    fn poll(&mut self) -> Poll<Option<Bytes>, Self::Error> {
        use self::SubscribeStream::*;

        let (bytes_opt, income) = match self {
            &mut SendingConsumeMethod(ref mut fut) => {
                let (income, _outcome) = try_ready!(fut.poll());
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
