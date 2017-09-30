use tokio_core::reactor::Handle;
use futures::{Future, Stream};

use bytes::Bytes;

use amqpr_api::socket::open as socket_open;
use amqpr_api::methods::{exchange, basic, queue};

use std::net::SocketAddr;

const SUBSCRIBER_CHANNEL: u16 = 21_u16;

pub fn subscribe_stream(exchange_name: String,
                        addr: SocketAddr,
                        user: String,
                        pass: String,
                        handle: Handle)
                        -> Box<Stream<Item = Bytes, Error = ()>> {

    let stream = socket_open(&addr, &handle, user, pass)
        .and_then(|global_con| global_con.declare_local_channel(SUBSCRIBER_CHANNEL))
        .and_then(|(g, l_con)| l_con.init().map(move |l| (g, l)))
        .map_err(|canceld| {
            error!("Canceled!!! : {:?}", canceld);
        })
        .and_then(move |(_, local_con)| {
            // Declare Exchange
            let declare_args = exchange::DeclareArguments {
                exchange_name: exchange_name.clone(),
                exchange_type: "fanout".into(),
                auto_delete: false,
                ..Default::default()
            };
            local_con.declare_exchange(declare_args);

            // Declare Queue
            let declare_queue_args = queue::DeclareArguments {
                queue_name: "",
                durable: true,
                exclusive: true,
                auto_delete: true,
                ..Default::default()
            };
            local_con.declare_queue(declare_queue_args)
                .map_err(|canceld| {
                    error!("Canceled!!! : {:?}", canceld);
                })
                .map(move |queue_name| {
                    // Bind Queue
                    let bind_args = queue::BindArguments {
                        queue_name: queue_name.clone(),
                        exchange_name: exchange_name,
                        ..Default::default()
                    };
                    local_con.bind_queue(bind_args);

                    // Consume items from queue.
                    let consume_args = basic::ConsumeArguments {
                        queue_name: queue_name,
                        no_ack: true,
                        exclusive: true,
                        ..Default::default()
                    };
                    local_con.consume(consume_args)
                })
        })
        .flatten_stream();

    Box::new(stream)
}
