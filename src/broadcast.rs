//! Convenience method to broadcast

use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;

use futures::sync::mpsc::{unbounded, UnboundedSender};
use futures::{Future, Stream, Sink};

pub use bytes::Bytes;

use std::net::SocketAddr;
use std::io::Error as IoError;
use std::time::Duration;
use std::rc::Rc;

use amqpr_api::{AmqpSocket, start_handshake, declare_exchange, open_channel, publish};
use amqpr_api::exchange::declare::{ExchangeType, DeclareExchangeOption};
use amqpr_api::basic::publish::PublishOption;
use amqpr_api::handshake::SimpleHandshaker;
use amqpr_api::heartbeat::send_heartbeat;
use amqpr_api::errors::*;

use unsync::connect;


/// TODO : Determine local channel id by argument.
const LOCAL_CHANNEL_ID: u16 = 42;



pub type BroadcastError = ::futures::sync::mpsc::SendError<Bytes>;

/// Convenient method for broadcasting something.
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
    let (tx, rx) = unbounded();

    let handshaker = SimpleHandshaker {
        user: user.into(),
        pass: pass.into(),
        virtual_host: "/".into(),
    };

    let handle2 = handle.clone();

    let exchange_name1 = exchange_name.into();
    let exchange_name2 = exchange_name1.clone();

    let fut = connect(&addr, handshaker, &handle.clone())
        .and_then(move |global| {
            global.heartbeat(Duration::new(60, 0), &handle2);
            global.open_channel(LOCAL_CHANNEL_ID)
        })
        .and_then(|(global, local)| {
            let option = DeclareExchangeOption {
                name: exchange_name1,
                typ: ExchangeType::Fanout,
                is_passive: false,
                is_durable: false,
                is_auto_delete: false,
                is_internal: false,
                is_no_wait: false,
            };
            local.declare_exchange(option).map(|l| (global, l))
        })
        .and_then(|(global, local)| {
            let option = PublishOption {
                exchange: exchange_name2.clone(),
                routing_key: "".into(),
                is_mandatory: false,
                is_immediate: false,
            };
            local.publish_sink(option).send_all(rx.from_err::<Rc<Error>>())
        })
        .map(|_| ())
        .map_err(|_| ());

    handle.spawn(fut);

    /*
    let future = TcpStream::connect(&addr, &handle)
        .map_err(|e| Error::from(e))
        .and_then(|socket| start_handshake(handshaker, socket))
        .and_then(|socket| open_channel(socket, LOCAL_CHANNEL_ID))
        .and_then(|socket| {
            declare_exchange(socket, LOCAL_CHANNEL_ID, option)
        })
        .and_then(move |socket| {
            info!("Start broadcasting");
            enum Command {
                Heartbeat,
                Publish(Bytes),
            }

            let heartbeat_command =
                ::tokio_core::reactor::Interval::new(::std::time::Duration::new(60, 0), &handle2).unwrap()
                .map(|()| Command::Heartbeat);

            let publish_command = receiver.then(|r| Ok::<_, IoError>(r.unwrap())).map(
                |bytes| {
                    Command::Publish(bytes)
                },
            );

            let commands = heartbeat_command.select(publish_command);

            type SocketFuture = Box<Future<Item = AmqpSocket, Error = IoError>>;

            commands
                .fold(socket, move |socket, command| match command {
                    Command::Heartbeat => Box::new(send_heartbeat(socket)) as SocketFuture,
                    Command::Publish(bytes) => {
                        info!("broadcast {:?}", bytes);
                        let option = PublishOption {
                            exchange: exchange_name2.clone(),
                            routing_key: "".into(),
                            is_mandatory: false,
                            is_immediate: false,
                        };
                        Box::new(publish::<IoError>(socket, LOCAL_CHANNEL_ID, bytes, option)) as SocketFuture
                    }
                })
                .map_err(|e| Error::from(e))
        })
        .map(|_socket| ())
        .map_err(|e| panic!("Error while broadcasting! : {:?}", e));

    handle.spawn(future);

    sender
        */

    panic!();
}
