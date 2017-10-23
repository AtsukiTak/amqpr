extern crate amqpr;
extern crate futures;
extern crate tokio_core;
extern crate bytes;
extern crate clap;

use futures::Stream;
use tokio_core::reactor::Core;

use clap::{App, Arg};

use amqpr::subscribe_stream;

fn main() {
    let args = get_args();

    let mut core = Core::new().unwrap();

    let subscribe_stream = subscribe_stream(
        args.exchange,
        args.amqp_addr.parse().unwrap(),
        args.user,
        args.pass,
        core.handle(),
    );

    let futures = subscribe_stream.for_each(|bytes| {
        println!("{:?}", bytes);
        Ok(())
    });

    core.run(futures).unwrap();
}


struct Args {
    amqp_addr: String,
    exchange: String,
    user: String,
    pass: String,
}


fn get_args() -> Args {
    let matches = App::new("AMQP simple subscriber")
        .arg(
            Arg::with_name("amqp_addr")
                .short("a")
                .long("amqp_addr")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("amqp_exchange")
                .short("e")
                .long("amqp_exchange")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("user")
                .short("u")
                .long("user")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("pass")
                .short("p")
                .long("pass")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    Args {
        amqp_addr: matches.value_of("amqp_addr").unwrap().into(),
        exchange: matches.value_of("amqp_exchange").unwrap().into(),
        user: matches.value_of("user").unwrap().into(),
        pass: matches.value_of("pass").unwrap().into(),
    }
}
