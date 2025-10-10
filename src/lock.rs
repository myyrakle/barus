use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

// A simple try-lock implementation using AtomicBool
#[derive(Clone)]
pub struct TryLock {
    locked: Arc<AtomicBool>,
}

pub struct LockGuard<'a> {
    lock: &'a TryLock,
}

impl<'a> LockGuard<'a> {
    pub fn new(lock: &'a TryLock) -> Self {
        Self { lock }
    }
}

impl Drop for LockGuard<'_> {
    fn drop(&mut self) {
        self.lock.unlock();
    }
}

impl Default for TryLock {
    fn default() -> Self {
        Self::new()
    }
}

impl TryLock {
    pub fn new() -> Self {
        Self {
            locked: Arc::new(AtomicBool::new(false)),
        }
    }

    // Attempt to acquire the lock. Returns Some(LockGuard) if successful, None if already locked.
    pub fn try_lock_guard(&self) -> Option<LockGuard<'_>> {
        if !self.locked.swap(true, Ordering::Acquire) {
            Some(LockGuard::new(self))
        } else {
            None
        }
    }

    // Attempt to acquire the lock. Returns true if successful, false if already locked.
    pub fn try_lock(&self) -> bool {
        !self.locked.swap(true, Ordering::Acquire)
    }

    // Release the lock.
    pub fn unlock(&self) {
        self.locked.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::TryLock;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    #[test]
    fn test_try_lock() {
        let lock = TryLock::new();
        assert!(lock.try_lock());
        assert!(!lock.try_lock());
        lock.unlock();
        assert!(lock.try_lock());
    }

    #[test]
    fn test_try_lock_guard() {
        let lock = TryLock::new();
        assert!(lock.try_lock_guard().is_some());

        {
            let _guard = lock.try_lock_guard();
            assert!(lock.try_lock_guard().is_none());
        }
        assert!(lock.try_lock_guard().is_some());
    }

    #[tokio::test]
    async fn test_try_lock_concurrently() {
        let lock = TryLock::new();
        let success_count = Arc::new(AtomicBool::new(false));
        let mut handles = vec![];

        // 10개의 태스크가 동시에 락을 획득하려고 시도
        for _ in 0..10 {
            let lock = lock.clone();
            let success_count = success_count.clone();
            let handle = tokio::spawn(async move {
                if let Some(_guard) = lock.try_lock_guard() {
                    // 락을 성공적으로 획득한 경우
                    success_count.store(true, Ordering::Relaxed);
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                    // _guard가 drop되면서 자동으로 unlock됨
                }
            });
            handles.push(handle);
        }

        // 모든 태스크 완료 대기
        for handle in handles {
            handle.await.unwrap();
        }

        // 적어도 하나의 태스크는 락을 획득했어야 함
        assert!(success_count.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn test_try_lock_sequential_access() {
        let lock = TryLock::new();
        let shared_value = Arc::new(std::sync::atomic::AtomicI32::new(0));
        let mut handles = vec![];

        // 순차적으로 락을 획득하고 공유 값을 증가시키는 테스트
        for _ in 0..5 {
            let lock = lock.clone();
            let shared_value = shared_value.clone();
            let handle = tokio::spawn(async move {
                // 락을 획득할 때까지 반복 시도
                loop {
                    if let Some(_guard) = lock.try_lock_guard() {
                        let current = shared_value.load(Ordering::Relaxed);
                        tokio::time::sleep(tokio::time::Duration::from_millis(1)).await;
                        shared_value.store(current + 1, Ordering::Relaxed);
                        break;
                    }
                    // 짧은 시간 대기 후 재시도
                    tokio::time::sleep(tokio::time::Duration::from_micros(100)).await;
                }
            });
            handles.push(handle);
        }

        // 모든 태스크 완료 대기
        for handle in handles {
            handle.await.unwrap();
        }

        // 모든 증가 연산이 완료되었는지 확인
        assert_eq!(shared_value.load(Ordering::Relaxed), 5);
    }
}
