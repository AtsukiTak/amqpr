use tokio_core::reactor::{Handle, Interval};
use tokio_core::net::TcpStream;
use futures::{Stream, Sink, Future, IntoFuture};
use futures::unsync::oneshot::{self, Receiver};

use ex_futures::{StreamExt, SinkExt};

use amqpr_codec::{Frame, FrameHeader, FramePayload};
use amqpr_api::channel::open::open_channel;
use amqpr_api::start_handshake;
use amqpr_api::handshake::Handshaker;

use std::time::Duration;
use std::net::SocketAddr;
use std::rc::Rc;

use super::{Income, Outcome};
use unsync::LocalChannel;
use errors::*;


pub fn connect<H: Handshaker + 'static>(
    addr: &SocketAddr,
    handshaker: H,
    handle: &Handle,
) -> GlobalChannelFuture {
    let fut = TcpStream::connect(addr, handle)
        .map_err(|e| Rc::new(Error::from(e)))
        .and_then(|socket| {
            start_handshake(handshaker, socket).map_err(|e| Rc::new(e))
        })
        .map(|socket| {
            let (outcome, income) = socket.split();
            let outcome: Box<Sink<SinkItem = Frame, SinkError = Rc<Error>>> =
                Box::new(outcome.sink_map_err(|e| Rc::new(Error::from(e))));
            let income: Box<Stream<Item = Frame, Error = Rc<Error>>> =
                Box::new(income.map_err(|e| Rc::new(Error::from(e))));
            GlobalChannel {
                income: income.unsync_cloneable(),
                outcome: outcome.unsync_cloneable(),
            }
        });
    Box::new(fut)
}



pub type GlobalChannelFuture = Box<Future<Item = GlobalChannel, Error = Rc<Error>>>;

pub type LocalChannelFuture = Box<Future<Item = (GlobalChannel, LocalChannel), Error = Rc<Error>>>;


pub struct GlobalChannel {
    income: Income,
    outcome: Outcome,
}


impl GlobalChannel {
    /// Open new local channel.
    /// Returned future's item is `(GloblChannel, LocalChannel)`.
    pub fn open_channel(self, channel_id: u16) -> LocalChannelFuture {
        let open = open_channel(self.income.clone(), self.outcome.clone(), channel_id);

        let fut = open.map(move |(income, outcome)| {
            let channel_id_ = channel_id;
            let local_income: Box<Stream<Item = Frame, Error = Rc<Error>>> =
                Box::new(income.filter(move |f| f.header.channel == channel_id_));
            let local_channel = LocalChannel {
                channel_id: channel_id,
                income: local_income.unsync_cloneable(),
                outcome: outcome.clone(),
            };
            (self, local_channel)
        });

        Box::new(fut)
    }


    /// Start sending heartbeat frame to AMQP server.
    pub fn heartbeat(&self, interval: Duration, handle: &Handle) -> Receiver<Rc<Error>> {
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

        let fut = interval.fold(self.outcome.clone(), move |sink, ()| {
            sink.send(heartbeat.clone())
        }).map_err(move |e| {
            let _ = tx.send(e); // We don't care whether success or not.
        }).map(|_| ());
        handle.spawn(fut);

        rx
    }
}
