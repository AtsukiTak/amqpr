extern crate amqpr_simple;
extern crate futures;
extern crate tokio_core;
extern crate bytes;
extern crate log4rs;

use futures::{future, stream, sync, Sink, Stream};
use tokio_core::reactor::{Core, Timeout};

use bytes::{BytesMut, BigEndian, BufMut};

use amqpr_simple::broadcast_sink;

use std::thread;
use std::time::Duration;


fn main() {
    log4rs::init_file("log4rs.yml", Default::default()).unwrap();


    let (sender, receiver) = sync::mpsc::unbounded();

    thread::spawn(move || {
        let mut n_2 = 0_u64;
        let mut n_1 = 1_u64;
        loop {
            thread::sleep_ms(1000);

            let n = n_2 + n_1;

            let mut bytes = BytesMut::new();
            bytes.reserve(8);
            bytes.put_u64::<BigEndian>(n);

            (&sender).send(bytes.freeze());

            n_2 = n_1;
            n_1 = n;
        }

    });


    let mut core = Core::new().unwrap();
    let handle = core.handle();


    let broadcast_sink = broadcast_sink("fibonacci".into(),
                                        "127.0.0.1:5672".parse().unwrap(),
                                        "guest".into(),
                                        "guest".into(),
                                        core.handle())
        .sink_map_err(|_| ());

    core.run(broadcast_sink.send_all(receiver));

    core.run(future::empty::<u8, u8>());
}
