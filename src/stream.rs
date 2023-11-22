use async_trait::async_trait;
use futures::stream::{repeat, Stream, StreamExt};
use futures::FutureExt;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::pin::{pin, Pin};
use std::sync::mpsc::Receiver;
use std::task::{Context, Poll};

#[async_trait]
pub trait PurgeableStream: Stream {
    async fn purge(&mut self);
}

#[async_trait]
pub trait Stage {
    type Input;
    type Output;

    async fn feed(&mut self, data: Self::Input);
    async fn next(&mut self) -> Option<Self::Output>;
}

#[async_trait]
pub trait PurgeableStage: Stage {
    async fn purge(&mut self);
}

pub struct StageStream<P, S, I, O> {
    prev: Pin<Box<P>>,
    stage: S,

    input_type: PhantomData<I>,
    output_type: PhantomData<O>,
}

impl<P, S, I, O> StageStream<P, S, I, O>
where
    P: Stream<Item = I> + Unpin,
    S: Stage<Input = I, Output = O> + Send,
{
    pub fn new(prev: P, stage: S) -> Self {
        Self {
            prev: Box::pin(prev),
            stage,

            input_type: PhantomData,
            output_type: PhantomData,
        }
    }

    async fn propagate(&mut self, prev_data: Option<I>) -> Option<O> {
        if let Some(data) = prev_data {
            self.stage.feed(data).await;
        }
        self.stage.next().await
    }
}

impl<P, S, I, O> Unpin for StageStream<P, S, I, O> {}

impl<P, S, I, O> Stream for StageStream<P, S, I, O>
where
    P: Stream<Item = I> + Unpin,
    S: Stage<Input = I, Output = O> + Send,
{
    type Item = O;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let prev_data = futures::ready!(self.prev.poll_next_unpin(cx));
        let mut fut = pin!(self.propagate(prev_data));
        if let Some(data) = futures::ready!(fut.poll_unpin(cx)) {
            return Poll::Ready(Some(data));
        }
        Poll::Ready(None)
    }
}

#[async_trait]
impl<P, S, I, O> PurgeableStream for StageStream<P, S, I, O>
where
    P: PurgeableStream<Item = I> + Unpin + Send,
    S: PurgeableStage<Input = I, Output = O> + Unpin + Send,
    I: Send,
    O: Send,
{
    async fn purge(&mut self) {
        self.prev.purge().await;
        self.stage.purge().await;
    }
}

#[async_trait]
impl<T: Clone + Send> PurgeableStream for futures::stream::Repeat<T> {
    async fn purge(&mut self) {}
}

struct ChannelStage<T> {
    rx: Receiver<T>,
}

impl<T> ChannelStage<T> {
    pub fn new(rx: Receiver<T>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl<T: Send> Stage for ChannelStage<T> {
    type Input = ();
    type Output = T;

    async fn feed(&mut self, _: Self::Input) {}

    async fn next(&mut self) -> Option<Self::Output> {
        self.rx.try_recv().ok()
    }
}

#[async_trait]
impl<T: Send> PurgeableStage for ChannelStage<T> {
    async fn purge(&mut self) {
        while self.rx.try_recv().is_ok() {}
    }
}

pub fn channel_stream<T: Send>(rx: Receiver<T>) -> impl PurgeableStream<Item = T> + Send {
    StageStream::new(repeat(()), ChannelStage::new(rx))
}

pub struct SyncFuncStage<F, I, O> {
    func: F,
    queue: VecDeque<O>,

    input_type: PhantomData<I>,
}

impl<F, I, O> SyncFuncStage<F, I, O>
where
    F: Fn(I) -> Vec<O>,
{
    pub fn new(func: F) -> Self {
        Self {
            func,
            queue: VecDeque::new(),
            input_type: PhantomData,
        }
    }
}

#[async_trait]
impl<F, I, O> Stage for SyncFuncStage<F, I, O>
where
    F: Fn(I) -> Vec<O> + Send,
    I: Send,
    O: Send,
{
    type Input = I;
    type Output = O;

    async fn feed(&mut self, data: Self::Input) {
        for data in (self.func)(data) {
            self.queue.push_back(data);
        }
    }

    async fn next(&mut self) -> Option<Self::Output> {
        self.queue.pop_front()
    }
}

#[async_trait]
impl<F, I, O> PurgeableStage for SyncFuncStage<F, I, O>
where
    F: Fn(I) -> Vec<O> + Send,
    I: Send,
    O: Send,
{
    async fn purge(&mut self) {
        self.queue.clear();
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::mpsc::channel;

    #[derive(Clone, Debug)]
    struct Data(u8);

    #[tokio::test]
    async fn test_stream() {
        let (tx, rx) = channel::<Data>();
        let channel_st = channel_stream(rx);
        let plus_ten_st = StageStream::new(
            channel_st,
            SyncFuncStage::new(|data: Data| vec![Data(data.0 + 10)]),
        );
        let mut plus_two_st = StageStream::new(
            plus_ten_st,
            SyncFuncStage::new(|data: Data| vec![Data(data.0 + 2)]),
        );

        tokio::spawn(async move {
            for i in 0..10 {
                let data = Data(i);
                tx.send(data).unwrap();
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        while let Some(data) = plus_two_st.next().await {
            println!("Received data: {:?}", data);
        }
    }
}
