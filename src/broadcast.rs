use tokio_core::reactor::Handle;
use futures::sync::mpsc::{unbounded, UnboundedSender};
use futures::{Future, Stream};

pub use bytes::Bytes;

use amqpr_api::socket::open as socket_open;
use amqpr_api::methods::{exchange, basic};

use std::net::SocketAddr;


pub type BroadcastError = ::futures::sync::mpsc::SendError<Bytes>;

pub fn broadcast_sink(exchange_name: String,
                      addr: SocketAddr,
                      user: String,
                      pass: String,
                      handle: Handle)
                      -> UnboundedSender<Bytes> {
    let (sender, receiver) = unbounded::<Bytes>();

    let handle2 = handle.clone();

    handle2.spawn_fn(move || {
        socket_open(&addr, &handle, user, pass)
            .and_then(|global_con| global_con.declare_local_channel(42))
            .and_then(|(g, local_con)| local_con.init().map(move |l| (g, l)))
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

                // Publish items.
                let pub_args = basic::PublishArguments {
                    exchange_name: exchange_name,
                    routing_key: "".into(),
                    mandatory: false,
                    ..Default::default()
                };
                receiver.for_each(move |bytes| {
                    local_con.publish(pub_args.clone(), bytes);
                    Ok(())
                })
            })
    });

    sender
}
