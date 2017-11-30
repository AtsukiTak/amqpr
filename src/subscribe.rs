use tokio_core::reactor::Handle;
use futures::{Future, Stream};

use bytes::Bytes;

use std::net::SocketAddr;
use std::time::Duration;
use std::rc::Rc;

use amqpr_api::handshake::SimpleHandshaker;

use errors::*;
use unsync::connect;


const LOCAL_CHANNEL_ID: u16 = 42;
const HEARTBEAT_SEC: u64 = 60;

pub type SubscribeStream = Box<Stream<Item = Bytes, Error = Rc<Error>>>;


pub fn subscribe_stream(
    exchange_name: String,
    addr: SocketAddr,
    user: String,
    pass: String,
    handle: Handle,
) -> SubscribeStream {

    let handshaker = SimpleHandshaker {
        user: user.into(),
        pass: pass.into(),
        virtual_host: "/".into(),
    };

    let stream = connect(&addr, handshaker, &handle.clone())
        .and_then(move |global| {
            info!("Handshake is finished");
            // Ignore error of heartbeat.
            let (global, _err_notify) = global.heartbeat(Duration::new(HEARTBEAT_SEC, 0), &handle);
            global.open_channel(LOCAL_CHANNEL_ID)
        })
        .and_then(|(_global, local)| {
            info!("A local channel open");
            local.declare_private_queue("")
        })
        .and_then(|(queue, local)| {
            info!("A private queue is declared");
            let queue_ = queue.clone();
            local.bind_queue(queue, exchange_name, "").map(|local| {
                (queue_, local)
            })
        })
        .map(|(queue, local)| {
            println!("Ready to subscribe");
            local.subscribe_stream(queue, "sub1")
        })
        .map(|(local, stream)| {
            drop(local);
            stream
        })
        .flatten_stream();

    Box::new(stream) as SubscribeStream
}
