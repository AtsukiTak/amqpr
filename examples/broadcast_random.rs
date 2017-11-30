extern crate amqpr;
extern crate futures;
extern crate tokio_core;
extern crate bytes;
extern crate clap;
extern crate rand;
#[macro_use]
extern crate log;

use futures::{Sink, Stream};
use tokio_core::reactor::{Core, Interval};

use bytes::{BytesMut, BigEndian, BufMut};

use clap::{App, Arg};

use amqpr::broadcast::broadcast_sink;

use std::time::Duration;

use rand::Rng;

const STREAM_INTERVAL_MILLI_SEC: u64 = 10;

fn main() {
    let (amqp_addr, user, pass) = get_args();

    let mut core = Core::new().unwrap();

    let mut rng = rand::OsRng::new().unwrap();
    let random_iter = rng.gen_iter::<u64>();
    let random_number_stream = futures::stream::iter_ok(random_iter);

    let interval_stream = Interval::new(
        Duration::from_millis(STREAM_INTERVAL_MILLI_SEC),
        &core.handle(),
    ).unwrap()
        .map_err(|e| println!("Error : {:?}", e));

    let mut bytes_mut = BytesMut::new();

    let bytes_stream = random_number_stream
            .zip(interval_stream)
            .map(move |(n,())| {
            bytes_mut.clear();
            bytes_mut.reserve(8);
            bytes_mut.put_u64::<BigEndian>(n);
            bytes_mut.clone().freeze()
        });

    let broadcast_sink_future = broadcast_sink(
        "random_num".into(),
        amqp_addr.parse().unwrap(),
        user,
        pass,
        core.handle(),
    );

    let broadcast_sink = core.run(broadcast_sink_future).unwrap();

    let broadcast_sink = broadcast_sink.sink_map_err(|e| error!("{:?}", e));

    core.run(broadcast_sink.send_all(bytes_stream)).unwrap();
}


fn get_args() -> (String, String, String) {
    let matches = App::new("AMQP random number broadcaster")
        .arg(
            Arg::with_name("amqp_addr")
                .short("a")
                .long("amqp_addr")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("user")
                .short("u")
                .long("user")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("pass")
                .short("p")
                .long("pass")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    (
        matches.value_of("amqp_addr").unwrap().into(),
        matches.value_of("user").unwrap().into(),
        matches.value_of("pass").unwrap().into(),
    )
}
