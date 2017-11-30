mod publish;
mod subscribe;

pub use self::publish::{PublishFuture, PublishSink, PublishOption};
pub use self::subscribe::{SubscribeStream, SubscribeOption};
pub use amqpr_api::exchange::declare::ExchangeType;

use futures::Future;

use ex_futures::sink::{SinkExt, UnsyncCloneable};

use bytes::Bytes;

use amqpr_api::exchange::declare::{declare_exchange_wait, DeclareExchangeOption};
use amqpr_api::queue::declare::{declare_queue_wait, DeclareQueueOption};
use amqpr_api::queue::bind::{bind_queue_wait, BindQueueOption};

use std::rc::Rc;

use super::{Income, Outgo, BoxedIncome};
use errors::*;


type LocalChannelFuture<In, Out> = Box<Future<Item = LocalChannel<In, Out>, Error = Rc<Error>>>;

type QueueName = String;
type DeclareQueueFuture<In, Out> = Box<
    Future<
        Item = (QueueName, LocalChannel<In, Out>),
        Error = Rc<Error>,
    >,
>;


pub struct LocalChannel<In: Income, Out: Outgo> {
    pub channel_id: u16,

    // Stream of Frame which channel id is same with above.
    pub(crate) income: In,

    // Sink of any kind of Frame. Should we restrict it?
    pub(crate) outgo: Out,
}


impl<In: Income, Out: Outgo> LocalChannel<In, Out> {
    /// Declare an exchange on AMQP server using default option.
    /// # Option
    /// - is_passive: false
    /// - is_durable: false
    /// - is_auto_delete: false
    /// - is_internal: false
    /// - is_no_wait: false
    pub fn declare_exchange<T>(self, exchange: T, typ: ExchangeType) -> LocalChannelFuture<In, Out>
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
    pub fn declare_exchange_with_option(
        self,
        option: DeclareExchangeOption,
    ) -> LocalChannelFuture<In, Out> {
        let channel_id = self.channel_id;
        let declared = declare_exchange_wait(self.income, self.outgo, self.channel_id, option);
        let fut = declared.map(move |(income, outgo)| {
            LocalChannel {
                channel_id: channel_id,
                income: income,
                outgo: outgo,
            }
        });

        Box::new(fut)
    }



    /// Declare a private queue. A private queue may only be accessed by the current
    /// connection, and are deleted when that connection close.
    /// You MAY NOT attempt to use a queue that was declared as private by another still-open
    /// connection.
    pub fn declare_private_queue<T>(self, name: T) -> DeclareQueueFuture<In, Out>
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
    pub fn declare_queue_with_option(
        self,
        option: DeclareQueueOption,
    ) -> DeclareQueueFuture<In, Out> {
        let ch_id = self.channel_id;
        // TODO : switch by no_wait flag
        let declared = declare_queue_wait(self.income, self.outgo, self.channel_id, option);
        let fut = declared.map(move |(res, income, outgo)| {
            let ch = LocalChannel {
                channel_id: ch_id,
                income: income,
                outgo: outgo,
            };
            (res.queue, ch)
        });

        Box::new(fut)
    }


    /// Bind a queue to AMQP server.
    pub fn bind_queue<S, T, U>(
        self,
        queue: S,
        exchange: T,
        routing_key: U,
    ) -> LocalChannelFuture<In, Out>
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
    pub fn bind_queue_with_option(self, option: BindQueueOption) -> LocalChannelFuture<In, Out> {
        let ch_id = self.channel_id;
        let bound = bind_queue_wait(self.income, self.outgo, self.channel_id, option);
        let fut = bound.map(move |(income, outgo)| {
            LocalChannel {
                channel_id: ch_id,
                income: income,
                outgo: outgo,
            }
        });

        Box::new(fut)
    }



    /// Publish an item to exchange.
    /// Maybe it is more useful to use `publish_sink` function instead.
    pub fn publish<S, T>(self, bytes: Bytes, exchange: S, routing_key: T) -> PublishFuture<In, Out>
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
    pub fn publish_with_option(
        self,
        bytes: Bytes,
        option: PublishOption,
    ) -> PublishFuture<In, Out> {
        self::publish::publish(self, bytes, option)
    }



    /// Get a outbound endpoint to publish items with default option.
    /// # Option
    /// - mandatory: false
    /// - immediate: false
    pub fn publish_sink<S, T>(
        self,
        exchange: S,
        routing_key: T,
    ) -> (LocalChannel<In, UnsyncCloneable<Out>>, PublishSink<UnsyncCloneable<Out>>)
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
    pub fn publish_sink_with_option(
        self,
        option: PublishOption,
    ) -> (LocalChannel<In, UnsyncCloneable<Out>>, PublishSink<UnsyncCloneable<Out>>) {
        let (id, income, outgo) = (self.channel_id, self.income, self.outgo);
        let cloneable_outgo = outgo.unsync_cloneable();
        let local_ch = LocalChannel {
            channel_id: id.clone(),
            income: income,
            outgo: cloneable_outgo.clone(),
        };
        let pub_sink = self::publish::publish_sink(id, option, cloneable_outgo);
        (local_ch, pub_sink)
    }




    /// Get an inbound stream of subscribed items.
    /// The stream you get is private. It means that only single stream is available for single queue.
    /// If you want "shared" stream, please look into `subscribe_shared_stream` function.
    pub fn subscribe_stream<S, T>(
        self,
        queue: S,
        consumer_tag: T,
    ) -> (LocalChannel<BoxedIncome, UnsyncCloneable<Out>>,
              SubscribeStream<BoxedIncome, UnsyncCloneable<Out>>)
    where
        S: Into<String>,
        T: Into<String>,
    {
        self::subscribe::subscribe_stream(
            self,
            queue.into(),
            consumer_tag.into(),
            false, // no_local
            true, // exclusibe
        )
    }



    /// Get an inbound stream of subscribed items.
    /// The stream you get is shared. It means that many stream are available for single queue.
    /// If you want "private" stream, please look into `subscribe_stream` function.
    pub fn subscribe_shared_stream<S, T>(
        self,
        queue: S,
        consumer_tag: T,
    ) -> (LocalChannel<BoxedIncome, UnsyncCloneable<Out>>,
              SubscribeStream<BoxedIncome, UnsyncCloneable<Out>>)
    where
        S: Into<String>,
        T: Into<String>,
    {
        self::subscribe::subscribe_stream(
            self,
            queue.into(),
            consumer_tag.into(),
            false, // no_local
            false, // exclusibe
        )
    }
}
