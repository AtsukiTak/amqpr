mod publish;
mod subscribe;

pub use self::publish::{PublishFuture, PublishSink, PublishOption};
pub use self::subscribe::{SubscribeStream, SubscribeOption};

use futures::{Stream, Future};

use ex_futures::StreamExt;

use bytes::Bytes;

use amqpr_api::exchange::declare::{declare_exchange_wait, DeclareExchangeOption, ExchangeType};
use amqpr_api::queue::declare::{declare_queue_wait, DeclareQueueOption};

use std::rc::Rc;

use super::{Income, Outcome};
use errors::*;



type DeclareExchangeFuture = Box<Future<Item = LocalChannel, Error = Rc<Error>>>;

type QueueName = String;
type DeclareQueueFuture = Box<Future<Item = (QueueName, LocalChannel), Error = Rc<Error>>>;


pub struct LocalChannel {
    pub channel_id: u16,

    // Stream of Frame which channel id is same with above.
    pub(crate) income: Income,

    // Sink of any kind of Frame. Should we restrict it?
    pub(crate) outcome: Outcome,
}


impl LocalChannel {
    /// Declare an exchange on AMQP server using default option.
    /// # Option
    /// - is_passive: true
    /// - is_durable: false
    /// - is_auto_delete: false
    /// - is_internal: false
    /// - is_no_wait: true
    pub fn declare_exchange<T>(self, exchange: T, typ: ExchangeType) -> DeclareExchangeFuture
    where
        T: Into<String>,
    {
        let option = DeclareExchangeOption {
            name: exchange.into(),
            typ: typ,
            is_passive: true,
            is_durable: false,
            is_auto_delete: false,
            is_internal: false,
            is_no_wait: true,
        };

        self.declare_exchange_with_option(option)
    }


    /// Declare an exchange on AMQP server with option.
    pub fn declare_exchange_with_option(
        self,
        option: DeclareExchangeOption,
    ) -> DeclareExchangeFuture {
        let channel_id = self.channel_id;
        let declared = declare_exchange_wait(self.income, self.outcome, self.channel_id, option);
        let fut = declared.map(move |(income, outcome)| {
            LocalChannel {
                channel_id: channel_id,
                income: income,
                outcome: outcome,
            }
        });

        Box::new(fut)
    }



    /// Declare a private queue. A private queue may only be accessed by the current
    /// connection, and are deleted when that connection close.
    /// You MAY NOT attempt to use a queue that was declared as private by another still-open
    /// connection.
    pub fn declare_private_queue<T>(self, name: T) -> DeclareQueueFuture
    where
        T: Into<String>,
    {
        let option = DeclareQueueOption {
            name: name.into(),
            is_passive: true,
            is_durable: false,
            is_exclusive: true,
            is_auto_delete: false,
            is_no_wait: true,
        };

        self.declare_queue_with_option(option)
    }



    /// Declare a queue on AMQO server with option.
    pub fn declare_queue_with_option(self, option: DeclareQueueOption) -> DeclareQueueFuture {
        let ch_id = self.channel_id;
        // TODO : switch by no_wait flag
        let declared = declare_queue_wait(self.income, self.outcome, self.channel_id, option);
        let fut = declared.map(move |(res, income, outcome)| {
            let ch = LocalChannel {
                channel_id: ch_id,
                income: income,
                outcome: outcome,
            };
            (res.queue, ch)
        });

        Box::new(fut)
    }


    /// Publish new item to exchange specified at option.
    pub fn publish(self, bytes: Bytes, option: PublishOption) -> PublishFuture {
        self::publish::publish(self, bytes, option)
    }


    /// Get a `Sink` of `Bytes`.
    pub fn publish_sink(&self, option: PublishOption) -> PublishSink {
        self::publish::publish_sink(self.channel_id, option, self.outcome.clone())
    }


    /// Get a `Stream` of `Bytes`.
    /// That stream represents stream of item sent from AMQP server.
    pub fn subscribe_stream(&self, option: SubscribeOption) -> SubscribeStream {
        self::subscribe::subscribe_stream(
            self.channel_id,
            option,
            self.income.clone(),
            self.outcome.clone(),
        )
    }
}
