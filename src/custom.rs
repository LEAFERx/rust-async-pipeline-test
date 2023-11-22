use async_trait::async_trait;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::mpsc::Receiver;

#[async_trait]
pub trait AsyncIter {
    type Item;

    async fn next(&mut self) -> Option<Self::Item>;
}

#[async_trait]
pub trait PurgeableAsyncIter: AsyncIter {
    async fn purge(&mut self);
}

pub struct ChannelStage<T> {
    rx: Receiver<T>,
}

impl<T> ChannelStage<T> {
    pub fn new(rx: Receiver<T>) -> Self {
        Self { rx }
    }
}

#[async_trait]
impl<T: Send> AsyncIter for ChannelStage<T> {
    type Item = T;

    async fn next(&mut self) -> Option<Self::Item> {
        self.rx.try_recv().ok()
    }
}

#[async_trait]
impl<T: Send> PurgeableAsyncIter for ChannelStage<T> {
    async fn purge(&mut self) {
        while self.rx.try_recv().is_ok() {}
    }
}

pub struct SyncFuncStage<S, F, I, O> {
    prev: S,
    func: F,
    queue: VecDeque<O>,

    input_type: PhantomData<I>,
}

impl<S, F, I, O> SyncFuncStage<S, F, I, O>
where
    S: AsyncIter<Item = I>,
    F: Fn(I) -> Vec<O>,
{
    pub fn new(prev: S, func: F) -> Self {
        Self {
            prev,
            func,
            queue: VecDeque::new(),
            input_type: PhantomData,
        }
    }
}

#[async_trait]
impl<S, F, I, O> AsyncIter for SyncFuncStage<S, F, I, O>
where
    S: AsyncIter<Item = I> + Send,
    F: Fn(I) -> Vec<O> + Send,
    I: Send,
    O: Send,
{
    type Item = O;

    async fn next(&mut self) -> Option<Self::Item> {
        if let Some(input) = self.prev.next().await {
            for output in (self.func)(input) {
                self.queue.push_back(output);
            }
        }
        self.queue.pop_front()
    }
}

#[async_trait]
impl<S, F, I, O> PurgeableAsyncIter for SyncFuncStage<S, F, I, O>
where
    S: AsyncIter<Item = I> + Send,
    F: Fn(I) -> Vec<O> + Send,
    I: Send,
    O: Send,
{
    async fn purge(&mut self) {
        while self.prev.next().await.is_some() {}
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
    async fn test_async_iter() {
        let (tx, rx) = channel::<Data>();
        let channel_stage = ChannelStage::new(rx);
        let plus_ten_stage = SyncFuncStage::new(channel_stage, |data| vec![Data(data.0 + 10)]);
        let mut plus_two_stage = SyncFuncStage::new(plus_ten_stage, |data| vec![Data(data.0 + 2)]);

        tokio::spawn(async move {
            for i in 0..10 {
                let data = Data(i);
                tx.send(data).unwrap();
            }
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        while let Some(data) = plus_two_stage.next().await {
            println!("Received data: {:?}", data);
        }
    }
}
