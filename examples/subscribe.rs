extern crate amqpr_simple;
extern crate futures;
extern crate tokio_core;
extern crate bytes;
extern crate log4rs;

use futures::{future, stream, sync, Sink, Stream};
use tokio_core::reactor::{Core, Timeout};

use bytes::{BytesMut, BigEndian, BufMut, Buf};

use amqpr_simple::subscribe_stream;

use std::thread;
use std::time::Duration;
use std::io::Cursor;


fn main() {
    log4rs::init_file("log4rs.yml", Default::default()).unwrap();

    let mut core = Core::new().unwrap();
    let handle = core.handle();

    let subscribe_stream = subscribe_stream("fibonacci".into(),
                                            "127.0.0.1:5672".parse().unwrap(),
                                            "guest".into(),
                                            "guest".into(),
                                            core.handle());

    let futures = subscribe_stream.for_each(|bytes| {
        let n = Cursor::new(bytes).get_u64::<BigEndian>();
        println!("=========================");
        println!("    {}    ", n);
        println!("=========================");
        Ok(())
    });

    core.run(futures);
}
