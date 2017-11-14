use tokio_core::reactor::Core;
use futures::{Future, Stream, Sink};
use futures::sync::mpsc::{channel, Receiver};

use bytes::Bytes;

use std::net::SocketAddr;
use std::time::Duration;

use amqpr_api::handshake::SimpleHandshaker;
use amqpr_api::errors::*;

use unsync::connect;


const LOCAL_CHANNEL_ID: u16 = 42;
const HEARTBEAT_SEC: u64 = 60;
const BUFFER: usize = 16;

pub fn subscribe_stream(
    exchange_name: String,
    addr: SocketAddr,
    user: String,
    pass: String,
) -> Receiver<Bytes> {

    let (tx, rx) = channel(BUFFER);

    ::std::thread::spawn(move || {

        let handshaker = SimpleHandshaker {
            user: user.into(),
            pass: pass.into(),
            virtual_host: "/".into(),
        };

        let mut core = Core::new().unwrap();
        let handle = core.handle();

        let fut = connect(&addr, handshaker, &core.handle())
            .and_then(move |global| {
                // Ignore error of heartbeat.
                let _err_notify = global.heartbeat(Duration::new(HEARTBEAT_SEC, 0), &handle);
                global.open_channel(LOCAL_CHANNEL_ID)
            })
            .and_then(|(_global, local)| {
                local.declare_private_queue("")
            })
            .and_then(|(queue, local)| {
                let queue_ = queue.clone();
                local.bind_queue(queue, exchange_name, "").map(|local| {
                    (queue_, local)
                })
            });

        let (queue, local) = core.run(fut).unwrap();

        println!("Ready to subscribe");

        let stream = local.subscribe_stream(queue, "").map_err(
            |e| error!("{:?}", e),
        );
        let fut = tx.sink_map_err(|_e| {
            info!("Subscribe stream is droped before complete to send")
        }).send_all(stream);

        core.run(fut).unwrap();
    });

    rx
}
