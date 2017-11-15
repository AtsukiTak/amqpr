//! Convenience method to broadcast

use tokio_core::reactor::Core;

use futures::sync::mpsc::{channel, Sender};
use futures::{Future, Stream, Sink};

pub use bytes::Bytes;

use std::net::SocketAddr;
use std::time::Duration;
use std::rc::Rc;

use amqpr_api::exchange::declare::ExchangeType;
use amqpr_api::handshake::SimpleHandshaker;
use amqpr_api::errors::*;

use unsync::connect;


const LOCAL_CHANNEL_ID: u16 = 42;
const HEARTBEAT_SEC: u64 = 60;
const BUFFER: usize = 16;


pub type BroadcastError = ::futures::sync::mpsc::SendError<Bytes>;

/// Convenient method for broadcasting something.
/// This function spawns new thread dedicating broadcasting to AMQP server.
/// If any error is occured, it panic.
pub fn broadcast_sink(
    exchange_name: String,
    addr: SocketAddr,
    user: String,
    pass: String,
) -> Sender<Bytes> {
    let (tx, rx) = channel(BUFFER);

    ::std::thread::spawn(move || {
        let handshaker = SimpleHandshaker {
            user: user,
            pass: pass,
            virtual_host: "/".into(),
        };

        let exchange_name2 = exchange_name.clone();

        let mut core = Core::new().unwrap();

        let handle = core.handle();

        let fut = connect(&addr, handshaker, &core.handle())
            .and_then(move |global| {
                // Ignore error of heartbeat.
                let _err_notify = global.heartbeat(Duration::new(HEARTBEAT_SEC, 0), &handle);
                global.open_channel(LOCAL_CHANNEL_ID)
            })
            .and_then(|(_global, local)| {
                local.declare_exchange(exchange_name, ExchangeType::Fanout)
            })
            .and_then(|local| {
                // Send all received data.
                let sink = local.publish_sink(exchange_name2, "");
                let stream = rx.then(|never_err| Ok::<_, Rc<Error>>(never_err.unwrap()));
                sink.send_all(stream)
            });

        core.run(fut).unwrap();

        info!("Complete to send all datas");
    });


    tx
}
