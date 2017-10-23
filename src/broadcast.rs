//! Convenience method to broadcast

use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;

use futures::sync::mpsc::{unbounded, UnboundedSender};
use futures::{Future, Stream};

pub use bytes::Bytes;

use std::net::SocketAddr;
use std::io::Error as IoError;

use amqpr_api::{start_handshake, declare_exchange, open_channel, publish};
use amqpr_api::exchange::declare::{ExchangeType, DeclareExchangeOption};
use amqpr_api::basic::publish::PublishOption;
use amqpr_api::handshake::SimpleHandshaker;
use amqpr_api::errors::*;


/// TODO : Determine local channel id by argument.
const LOCAL_CHANNEL_ID: u16 = 42;



pub type BroadcastError = ::futures::sync::mpsc::SendError<Bytes>;

pub fn broadcast_sink<T, U, V>(
    exchange_name: T,
    addr: SocketAddr,
    user: U,
    pass: V,
    handle: Handle,
) -> UnboundedSender<Bytes>
where
    T: Into<String> + 'static,
    U: Into<String> + 'static,
    V: Into<String> + 'static,
{

    let (sender, receiver) = unbounded::<Bytes>();

    let handshaker = SimpleHandshaker {
        user: user.into(),
        pass: pass.into(),
        virtual_host: "/".into(),
    };

    let exchange_name1 = exchange_name.into();
    let exchange_name2 = exchange_name1.clone();

    let future = TcpStream::connect(&addr, &handle)
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
        .and_then(move |socket| {
            info!("Start broadcasting");
            receiver
                .then(|r| Ok::<_, IoError>(r.unwrap()))
                .fold(socket, move |socket, bytes| {
                    info!("broadcast {:?}", bytes);
                    let option = PublishOption {
                        exchange: exchange_name2.clone(),
                        routing_key: "".into(),
                        is_mandatory: false,
                        is_immediate: false,
                    };
                    publish::<IoError>(socket, LOCAL_CHANNEL_ID, bytes, option)
                })
                .map_err(|e| Error::from(e))
        })
        .map(|_socket| ())
        .map_err(|e| panic!("Error while broadcasting! : {:?}", e));

    handle.spawn(future);

    sender
}
