//! Convenience method to broadcast

use tokio_core::reactor::Handle;

use futures::{Future, Sink};

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


pub type BroadcastSink = Box<Sink<SinkItem = Bytes, SinkError = Rc<Error>> + 'static>;
pub type BroadcastSinkFuture = Box<Future<Item = BroadcastSink, Error = Rc<Error>> + 'static>;


/// Convenient method for broadcasting something.
/// This function spawns new thread dedicating broadcasting to AMQP server.
/// If any error is occured, it panic.
pub fn broadcast_sink(
    exchange_name: String,
    addr: SocketAddr,
    user: String,
    pass: String,
    handle: Handle,
) -> BroadcastSinkFuture {
    let handshaker = SimpleHandshaker {
        user: user,
        pass: pass,
        virtual_host: "/".into(),
    };

    let exchange_name2 = exchange_name.clone();

    let sink_fut = connect(&addr, handshaker, &handle.clone())
        .map(move |global| {
            info!("Handshake is finished");
            global.heartbeat(Duration::new(HEARTBEAT_SEC, 0), &handle)
        })
        .and_then(|(global, _heartbeat_error_notify)| {
            drop(_heartbeat_error_notify); // Ignore error of heartbeat.
            global.open_channel(LOCAL_CHANNEL_ID)
        })
        .and_then(|(_global, local)| {
            drop(_global);
            info!("Local channel open");
            local.declare_exchange(exchange_name, ExchangeType::Fanout)
        })
        .map(|local| {
            info!("An exchange is declared");
            local.publish_sink(exchange_name2, "")
        })
        .map(|(_local, sink)| Box::new(sink) as BroadcastSink);

    Box::new(sink_fut) as BroadcastSinkFuture
}
