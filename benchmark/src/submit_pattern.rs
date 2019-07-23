use logger::prelude::*;
use std::{thread, time, vec::IntoIter};

/// For constant rate X items per second,
/// ConstantRate iterator returns 1 item and then sleep for SUBMISSION_TIME_UNIT_US / X us.
const SUBMISSION_TIME_UNIT_US: u64 = 1_000_000;

/// Utilize IntoIter to implement an iterator that return items at constant speed.
/// That is, an item is returned to user after waiting some time duration.
/// Return immediately (e.g. flood all available items) when rate is set to u64::MAX.
#[derive(Debug, Clone)]
pub struct ConstantRate<T> {
    /// Number of items to return per second.
    constant: u64,
    /// Time duration to return 1 item.
    interval_us: u64,
    /// After return the item, user may perform other operations on the item,
    /// which consume some time duration X.
    /// User can set this variable to X so that we can exclude X from the waiting duration.
    exclude_duration_us: u64,
    /// Pointer to the items to be consumed.
    into_iter: IntoIter<T>,
}

impl<T> ConstantRate<T> {
    /// Init with constant rate, and the IntoIter of the vector to be consumed.
    pub fn new(constant: u64, into_iter: IntoIter<T>) -> Self {
        assert!(constant > 0);
        let interval_us = SUBMISSION_TIME_UNIT_US / constant;
        debug!("Return 1 item after {} us", interval_us);
        ConstantRate {
            constant,
            interval_us,
            exclude_duration_us: 0,
            into_iter,
        }
    }

    pub fn set_exclude_duration_us(&mut self, duration_us: u64) {
        self.exclude_duration_us = duration_us;
    }
}

impl<T> Iterator for ConstantRate<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        if self.interval_us > self.exclude_duration_us {
            let rest = self.interval_us - self.exclude_duration_us;
            debug!("Sleep {} us before return next item", rest);
            thread::sleep(time::Duration::from_micros(rest as u64));
        }
        self.into_iter.next()
    }
}

#[cfg(test)]
mod tests {
    use crate::submit_pattern::ConstantRate;
    use std::time;

    #[test]
    fn test_empty_vec() {
        let empty_vec: Vec<u32> = vec![];
        let mut pattern = ConstantRate::new(1, empty_vec.into_iter());
        let item = pattern.next();
        assert_eq!(item.is_none(), true);
    }

    #[test]
    fn test_flood_all() {
        let vec = vec![1, 2, 3, 4, 5, 6, 7];
        let flood = ConstantRate::new(std::u64::MAX, vec.into_iter());

        let now = time::Instant::now();
        for item in flood {
            let elapsed = now.elapsed().as_micros();
            println!("Ret {:?} after {:?} us", item, elapsed);
        }
        let elapsed = now.elapsed().as_micros();
        // Loop should finish at an glimpse.
        assert!(elapsed < 1000);
    }

    #[test]
    fn test_contant_rate() {
        let vec = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let const_rate = ConstantRate::new(2, vec.into_iter());

        let mut now = time::Instant::now();
        for item in const_rate {
            let new_now = time::Instant::now();
            let delta = new_now.duration_since(now).as_micros();
            println!("Ret {:?} after {:?} us", item, delta);
            // Interval between each call to next() should be roughly 0.5 second.
            assert!(delta < 510_000);
            assert!(delta > 490_000);
            now = new_now;
        }
    }
}
