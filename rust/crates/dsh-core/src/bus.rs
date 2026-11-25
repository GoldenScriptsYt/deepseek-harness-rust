//! Native replacement for Cordis `ctx.on()` / `ctx.effect()`: ordered listeners that
//! unsubscribe when their registration handle drops, plus a waterfall chain where each
//! listener must call `next` to delegate.

use std::sync::{Arc, Mutex};

type Listener<E> = Arc<dyn Fn(&E) + Send + Sync>;

struct Inner<E> {
    next_id: u64,
    listeners: Vec<(u64, Listener<E>)>,
}

/// Broadcast bus. Listeners run in registration order on the emitting task.
pub struct Bus<E> {
    inner: Arc<Mutex<Inner<E>>>,
}

impl<E> Clone for Bus<E> {
    fn clone(&self) -> Self {
        Bus {
            inner: self.inner.clone(),
        }
    }
}

impl<E> Default for Bus<E> {
    fn default() -> Self {
        Bus {
            inner: Arc::new(Mutex::new(Inner {
                next_id: 0,
                listeners: Vec::new(),
            })),
        }
    }
}

/// Dropping a registration removes its listener, as a Cordis effect disposer does.
#[must_use = "dropping the registration unsubscribes the listener"]
pub struct Registration<E> {
    inner: Arc<Mutex<Inner<E>>>,
    id: u64,
}

impl<E> Drop for Registration<E> {
    fn drop(&mut self) {
        self.inner
            .lock()
            .unwrap()
            .listeners
            .retain(|(id, _)| *id != self.id);
    }
}

impl<E> Bus<E> {
    pub fn on(&self, f: impl Fn(&E) + Send + Sync + 'static) -> Registration<E> {
        let mut g = self.inner.lock().unwrap();
        let id = g.next_id;
        g.next_id += 1;
        g.listeners.push((id, Arc::new(f)));
        Registration {
            inner: self.inner.clone(),
            id,
        }
    }

    pub fn emit(&self, event: &E) {
        // Snapshot so listeners may subscribe or drop registrations while running.
        let snapshot: Vec<Listener<E>> = self
            .inner
            .lock()
            .unwrap()
            .listeners
            .iter()
            .map(|(_, l)| l.clone())
            .collect();
        for l in snapshot {
            l(event);
        }
    }

    pub fn listener_count(&self) -> usize {
        self.inner.lock().unwrap().listeners.len()
    }
}

/// Continuation handed to a waterfall listener.
pub struct Next<'a, T> {
    rest: &'a [WaterfallFn<T>],
}

type WaterfallFn<T> = Arc<dyn for<'a> Fn(T, Next<'a, T>) -> T + Send + Sync>;

impl<T> Next<'_, T> {
    /// Delegate to the next listener, or return `value` unchanged at the end of the chain.
    pub fn call(self, value: T) -> T {
        match self.rest.split_first() {
            Some((head, tail)) => head(value, Next { rest: tail }),
            None => value,
        }
    }
}

/// Ordered chain; a listener that does not call `next` short-circuits the rest.
pub struct Waterfall<T> {
    chain: Mutex<Vec<WaterfallFn<T>>>,
}

impl<T> Default for Waterfall<T> {
    fn default() -> Self {
        Waterfall {
            chain: Mutex::new(Vec::new()),
        }
    }
}

impl<T> Waterfall<T> {
    pub fn on(&self, f: impl for<'a> Fn(T, Next<'a, T>) -> T + Send + Sync + 'static) {
        self.chain.lock().unwrap().push(Arc::new(f));
    }

    pub fn run(&self, value: T) -> T {
        let chain: Vec<WaterfallFn<T>> = self.chain.lock().unwrap().clone();
        Next { rest: &chain }.call(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn listeners_run_in_order_and_unsubscribe_on_drop() {
        let bus: Bus<u32> = Bus::default();
        let log = Arc::new(Mutex::new(Vec::new()));
        let (l1, l2) = (log.clone(), log.clone());
        let r1 = bus.on(move |e| l1.lock().unwrap().push(("a", *e)));
        let _r2 = bus.on(move |e| l2.lock().unwrap().push(("b", *e)));
        bus.emit(&1);
        drop(r1);
        bus.emit(&2);
        assert_eq!(*log.lock().unwrap(), vec![("a", 1), ("b", 1), ("b", 2)]);
        assert_eq!(bus.listener_count(), 1);
    }

    #[test]
    fn listener_may_unsubscribe_during_emit() {
        let bus: Bus<()> = Bus::default();
        let hits = Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let slot: Arc<Mutex<Option<Registration<()>>>> = Arc::new(Mutex::new(None));
        let s = slot.clone();
        *slot.lock().unwrap() = Some(bus.on(move |_| {
            h.fetch_add(1, Ordering::SeqCst);
            s.lock().unwrap().take();
        }));
        bus.emit(&());
        bus.emit(&());
        assert_eq!(hits.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn waterfall_delegates_and_short_circuits() {
        let w: Waterfall<i32> = Waterfall::default();
        w.on(|v, next| next.call(v + 1));
        w.on(|v, next| next.call(v * 10));
        assert_eq!(w.run(1), 20);

        let w: Waterfall<i32> = Waterfall::default();
        w.on(|v, _next| v + 100); // never delegates
        w.on(|v, next| next.call(v * 10));
        assert_eq!(w.run(1), 101);
    }
}
