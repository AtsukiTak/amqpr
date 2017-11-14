mod publish;
mod subscribe;

pub use self::publish::{PublishFuture, PublishSink, PublishOption};
pub use self::subscribe::{SubscribeStream, SubscribeOption};
pub use amqpr_api::exchange::declare::ExchangeType;

use futures::{Stream, Future};

use ex_futures::StreamExt;

use bytes::Bytes;

use amqpr_api::exchange::declare::{declare_exchange_wait, DeclareExchangeOption};
use amqpr_api::queue::declare::{declare_queue_wait, DeclareQueueOption};
use amqpr_api::queue::bind::{bind_queue_wait, BindQueueOption};

use std::rc::Rc;

use super::{Income, Outcome};
use errors::*;


type LocalChannelFuture = Box<Future<Item = LocalChannel, Error = Rc<Error>>>;

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
    /// - is_passive: false
    /// - is_durable: false
    /// - is_auto_delete: false
    /// - is_internal: false
    /// - is_no_wait: false
    pub fn declare_exchange<T>(self, exchange: T, typ: ExchangeType) -> LocalChannelFuture
    where
        T: Into<String>,
    {
        let option = DeclareExchangeOption {
            name: exchange.into(),
            typ: typ,
            is_passive: false,
            is_durable: false,
            is_auto_delete: false,
            is_internal: false,
            is_no_wait: false,
        };

        self.declare_exchange_with_option(option)
    }


    /// Declare an exchange on AMQP server with option.
    pub fn declare_exchange_with_option(self, option: DeclareExchangeOption) -> LocalChannelFuture {
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
            is_passive: false,
            is_durable: false,
            is_exclusive: true,
            is_auto_delete: false,
            is_no_wait: false,
        };

        self.declare_queue_with_option(option)
    }



    /// Declare a queue on AMQP server with option.
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


    /// Bind a queue to AMQP server.
    pub fn bind_queue<S, T, U>(self, queue: S, exchange: T, routing_key: U) -> LocalChannelFuture
    where
        S: Into<String>,
        T: Into<String>,
        U: Into<String>,
    {
        let option = BindQueueOption {
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            is_no_wait: false,
        };
        self.bind_queue_with_option(option)
    }


    /// Bind a queue to AMQP servier with option.
    pub fn bind_queue_with_option(self, option: BindQueueOption) -> LocalChannelFuture {
        let ch_id = self.channel_id;
        let bound = bind_queue_wait(self.income, self.outcome, self.channel_id, option);
        let fut = bound.map(move |(income, outcome)| {
            LocalChannel {
                channel_id: ch_id,
                income: income,
                outcome: outcome,
            }
        });

        Box::new(fut)
    }



    /// Publish an item to exchange.
    /// Maybe it is more useful to use `publish_sink` function instead.
    pub fn publish<S, T>(self, bytes: Bytes, exchange: S, routing_key: T) -> PublishFuture
    where
        S: Into<String>,
        T: Into<String>,
    {
        let option = PublishOption {
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            is_mandatory: false,
            is_immediate: false,
        };
        self.publish_with_option(bytes, option)
    }


    /// Publish an item to exchange specified by option.
    /// Maybe it is more useful to use `publish_sink_with_option` function instead.
    pub fn publish_with_option(self, bytes: Bytes, option: PublishOption) -> PublishFuture {
        self::publish::publish(self, bytes, option)
    }



    /// Get a outbound endpoint to publish items with default option.
    /// # Option
    /// - mandatory: false
    /// - immediate: false
    pub fn publish_sink<S, T>(&self, exchange: S, routing_key: T) -> PublishSink
    where
        S: Into<String>,
        T: Into<String>,
    {
        let option = PublishOption {
            exchange: exchange.into(),
            routing_key: routing_key.into(),
            is_mandatory: false,
            is_immediate: false,
        };
        self.publish_sink_with_option(option)
    }


    /// Get a outbound endpoint to publish items.
    pub fn publish_sink_with_option(&self, option: PublishOption) -> PublishSink {
        self::publish::publish_sink(self.channel_id, option, self.outcome.clone())
    }




    /// Get an inbound stream of subscribed items.
    /// The stream you get is private. It means that only single stream is available for single queue.
    /// If you want "shared" stream, please look into `subscribe_shared_stream` function.
    pub fn subscribe_stream<S, T>(&self, queue: S, consumer_tag: T) -> SubscribeStream
    where
        S: Into<String>,
        T: Into<String>,
    {
        self::subscribe::subscribe_stream(
            self.channel_id,
            queue.into(),
            consumer_tag.into(),
            false, // no_local
            true, // exclusibe
            self.income.clone(),
            self.outcome.clone(),
        )
    }



    /// Get an inbound stream of subscribed items.
    /// The stream you get is shared. It means that many stream are available for single queue.
    /// If you want "private" stream, please look into `subscribe_stream` function.
    pub fn subscribe_shared_stream<S, T>(&self, queue: S, consumer_tag: T) -> SubscribeStream
    where
        S: Into<String>,
        T: Into<String>,
    {
        self::subscribe::subscribe_stream(
            self.channel_id,
            queue.into(),
            consumer_tag.into(),
            false, // no_local
            false, // exclusibe
            self.income.clone(),
            self.outcome.clone(),
        )
    }
}
