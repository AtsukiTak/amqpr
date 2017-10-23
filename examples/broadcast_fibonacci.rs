extern crate amqpr;
extern crate futures;
extern crate tokio_core;
extern crate bytes;
extern crate clap;
#[macro_use]
extern crate log;
extern crate log4rs;

use futures::{Sink, Stream};
use tokio_core::reactor::{Core, Interval};

use bytes::{BytesMut, BigEndian, BufMut};

use clap::{App, Arg};

use amqpr::broadcast_sink;

use std::time::Duration;


fn main() {
    logger();

    let (amqp_addr, user, pass) = get_args();

    let mut core = Core::new().unwrap();

    let bytes_stream = {
        let fibonacci_stream =
            futures::stream::unfold((0u64, 1u64), |(n0, n1)| Some(Ok((n0 + n1, (n1, n0 + n1)))));

        let interval_stream = Interval::new(Duration::from_secs(1), &core.handle())
            .unwrap()
            .map_err(|e| println!("Error : {:?}", e));

        let mut bytes_mut = BytesMut::new();

        fibonacci_stream
            .zip(interval_stream)
            .map(move |(n,())| {
                bytes_mut.clear();
                bytes_mut.reserve(8);
                bytes_mut.put_u64::<BigEndian>(n);
                bytes_mut.clone().freeze()
            })
    };

    let broadcast_sink = broadcast_sink(
        "fibonacci",
        amqp_addr.parse().unwrap(),
        user,
        pass,
        core.handle(),
    ).sink_map_err(|_| ());

    core.run(broadcast_sink.send_all(bytes_stream)).unwrap();
}


fn get_args() -> (String, String, String) {
    let matches = App::new("AMQP fibonacci broadcaster")
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


fn logger() {
    use log::LogLevelFilter;
    use log4rs::append::console::ConsoleAppender;
    use log4rs::config::{Appender, Config, Root};
    let stdout = ConsoleAppender::builder().build();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .build(Root::builder().appender("stdout").build(
            LogLevelFilter::Info,
        ))
        .unwrap();

    log4rs::init_config(config).unwrap();
}
