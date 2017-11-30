use tokio_core::reactor::{Handle, Interval};
use tokio_core::net::TcpStream;
use futures::{Stream, Sink, Future, IntoFuture};
use futures::unsync::oneshot::{self, Receiver};

use ex_futures::{StreamExt, SinkExt};
use ex_futures::sink::UnsyncCloneable;

use amqpr_codec::{Frame, FrameHeader, FramePayload};
use amqpr_api::channel::open::open_channel;
use amqpr_api::start_handshake;
use amqpr_api::handshake::Handshaker;

use std::time::Duration;
use std::net::SocketAddr;
use std::rc::Rc;

use super::{Income, Outgo, BoxedIncome, BoxedOutgo, AmqpFuture};
use unsync::LocalChannel;
use errors::*;



pub fn connect<H: Handshaker + 'static>(
    addr: &SocketAddr,
    handshaker: H,
    handle: &Handle,
) -> Box<AmqpFuture<GlobalChannel<BoxedIncome, BoxedOutgo>>> {
    let fut = TcpStream::connect(addr, handle)
        .map_err(|e| Rc::new(Error::from(e)))
        .and_then(|socket| {
            start_handshake(handshaker, socket).map_err(|e| Rc::new(e))
        })
        .map(|socket| {
            let (outgo, income) = socket.split();
            let outgo: BoxedOutgo = Box::new(outgo.sink_map_err(|e| Rc::new(Error::from(e))));
            let income: BoxedIncome = Box::new(income.map_err(|e| Rc::new(Error::from(e))));
            GlobalChannel {
                income: income,
                outgo: outgo,
            }
        });
    Box::new(fut)
}



pub struct GlobalChannel<In: Income, Out: Outgo> {
    income: In,
    outgo: Out,
}


impl<In: Income, Out: Outgo> GlobalChannel<In, Out> {
    /// Open new local channel.
    /// Returned future's item is `(GlobalChannel, LocalChannel)`.
    pub fn open_channel(
        self,
        channel_id: u16,
    ) -> Box<
        AmqpFuture<
            (GlobalChannel<BoxedIncome, BoxedOutgo>,
             LocalChannel<BoxedIncome, BoxedOutgo>),
        >,
    > {
        let (income, outgo) = (self.income, self.outgo);
        let channel = channel_id.clone();
        let (l_in, g_in) = income.unsync_fork(move |f| f.header.channel == channel);
        let cloneable_outgo = outgo.unsync_cloneable();

        // Create GlobalChannel
        let global_channel = GlobalChannel {
            income: Box::new(g_in) as BoxedIncome,
            outgo: Box::new(cloneable_outgo.clone()) as BoxedOutgo,
        };

        let open = open_channel(l_in, cloneable_outgo.clone(), channel_id);

        let fut = open.map(move |(income, outgo)| {
            let local_channel = LocalChannel {
                channel_id: channel_id,
                income: Box::new(income) as BoxedIncome,
                outgo: Box::new(outgo) as BoxedOutgo,
            };
            (global_channel, local_channel)
        });

        Box::new(fut)
    }


    /// Start sending heartbeat frame to AMQP server.
    pub fn heartbeat(
        self,
        interval: Duration,
        handle: &Handle,
    ) -> (GlobalChannel<In, UnsyncCloneable<Out>>, Receiver<Rc<Error>>) {
        let interval = Interval::new(interval, handle)
            .into_future()
            .map_err(|e| Rc::new(Error::from(e)))
            .map(|stream| stream.map_err(|e| Rc::new(Error::from(e))))
            .flatten_stream();
        let heartbeat = Frame {
            header: FrameHeader { channel: 0 },
            payload: FramePayload::Heartbeat,
        };
        let (tx, rx) = oneshot::channel();

        let (income, outgo) = (self.income, self.outgo);
        let cloneable_outgo = outgo.unsync_cloneable();
        let global_channel = GlobalChannel {
            income: income,
            outgo: cloneable_outgo.clone(),
        };

        let fut = interval.fold(cloneable_outgo, move |sink, ()| {
            sink.send(heartbeat.clone())
        }).map_err(move |e| {
            let _ = tx.send(e); // We don't care whether success or not.
        }).map(|_| ());
        handle.spawn(fut);

        (global_channel, rx)
    }
}
