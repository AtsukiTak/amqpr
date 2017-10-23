use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
use futures::{Future, Stream};
use futures::stream::unfold;

use bytes::Bytes;

use std::net::SocketAddr;

use amqpr_api::{start_handshake, declare_exchange, open_channel, bind_queue, declare_queue,
                start_consume, receive_delivered};
use amqpr_api::exchange::declare::{ExchangeType, DeclareExchangeOption};
use amqpr_api::queue::{DeclareQueueOption, BindQueueOption};
use amqpr_api::basic::StartConsumeOption;
use amqpr_api::handshake::SimpleHandshaker;
use amqpr_api::errors::*;


const LOCAL_CHANNEL_ID: u16 = 42;

pub fn subscribe_stream<T, U, V>(
    exchange_name: T,
    addr: SocketAddr,
    user: U,
    pass: V,
    handle: Handle,
) -> Box<Stream<Item = Bytes, Error = Error>>
where
    T: Into<String> + 'static,
    U: Into<String> + 'static,
    V: Into<String> + 'static,
{

    let handshaker = SimpleHandshaker {
        user: user.into(),
        pass: pass.into(),
        virtual_host: "/".into(),
    };

    let exchange_name1 = exchange_name.into().clone();
    let exchange_name2 = exchange_name1.clone();

    let stream = TcpStream::connect(&addr, &handle)
        .map_err(|e| Error::from(e))
        .and_then(|socket| start_handshake(handshaker, socket))
        .and_then(|socket| open_channel(socket, LOCAL_CHANNEL_ID))
        .and_then(|socket| {
            let option = DeclareExchangeOption {
                name: exchange_name1,
                typ: ExchangeType::Fanout,
                is_passive: false,
                is_durable: false,
                is_auto_delete: false,
                is_internal: false,
                is_no_wait: false,
            };
            declare_exchange(socket, LOCAL_CHANNEL_ID, option)
        })
        .and_then(|socket| {
            let option = DeclareQueueOption {
                name: "".into(),
                is_passive: false,
                is_durable: false,
                is_exclusive: false,
                is_auto_delete: true,
                is_no_wait: false,
            };
            declare_queue(socket, LOCAL_CHANNEL_ID, option).map(
                |(queue_res, socket)| (queue_res.queue, socket),
            )
        })
        .and_then(|(queue, socket)| {
            let option = BindQueueOption {
                queue: queue.clone(),
                exchange: exchange_name2,
                routing_key: "".into(),
                is_no_wait: false,
            };
            bind_queue(socket, LOCAL_CHANNEL_ID, option).map(move |socket| (queue, socket))
        })
        .and_then(|(queue, socket)| {
            let option = StartConsumeOption {
                queue: queue,
                consumer_tag: "".into(),
                is_no_local: false,
                is_no_ack: true,
                is_exclusive: false,
                is_no_wait: false,
            };
            start_consume(socket, LOCAL_CHANNEL_ID, option)
        })
        .map(|socket| {
            unfold(socket, |socket| Some(receive_delivered(socket)))
        })
        .flatten_stream();

    Box::new(stream)
}
