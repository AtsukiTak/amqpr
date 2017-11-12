use futures::{Future, Stream, Poll, Async};

use amqpr_api::basic::consume::{start_consume, ConsumeStarted};
use amqpr_api::basic::deliver::{receive_delivered, Delivered};

use bytes::Bytes;

use std::rc::Rc;

use super::{Income, Outcome};
use errors::Error;


pub use amqpr_api::basic::consume::StartConsumeOption as SubscribeOption;



pub fn subscribe_stream(
    channel: u16,
    option: SubscribeOption,
    income: Income,
    outcome: Outcome,
) -> SubscribeStream {
    let consume_started = start_consume(income, outcome, channel, option);
    SubscribeStream::SendingConsumeMethod(consume_started)
}


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
