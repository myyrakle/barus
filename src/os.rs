use crate::errors;
use tokio::fs::File;

#[cfg(target_os = "linux")]
pub async fn file_resize_and_set_zero(file: &mut File, size: u32) -> errors::Result<()> {
    use std::os::fd::{AsFd, AsRawFd};

    let file_size = match file.metadata().await {
        Ok(metadata) => metadata.len(),
        Err(e) => {
            return Err(errors::Errors::FileOpenError(format!(
                "Failed to get file metadata: {}",
                e
            )));
        }
    };

    let fd = file.as_fd().as_raw_fd();

    let result = unsafe {
        libc::fallocate(
            fd,
            libc::FALLOC_FL_ZERO_RANGE, // 0으로 채우기
            file_size as i64,
            size as i64,
        )
    };

    if result != 0 {
        return Err(errors::Errors::FileOpenError(format!(
            "Failed to zero-fill new WAL segment file: {}",
            std::io::Error::last_os_error()
        )));
    }

    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub async fn file_resize_and_set_zero(file: &mut File, size: u32) -> Result<(), errors::Errors> {
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    let file_size = match file.metadata().await {
        Ok(metadata) => metadata.len(),
        Err(e) => {
            return Err(errors::Errors::FileOpenError(format!(
                "Failed to get file metadata: {}",
                e
            )));
        }
    };

    file.set_len(file_size + size as u64).await.map_err(|e| {
        errors::Errors::WALSegmentFileOpenError(format!(
            "Failed to set length for new WAL segment file: {}",
            e
        ))
    })?;

    let zero_bytes = vec![0; size as usize];

    file.seek(std::io::SeekFrom::Start(file_size))
        .await
        .map_err(|e| errors::Errors::WALSegmentFileOpenError(e.to_string()))?;

    file.write_all(&zero_bytes).await.map_err(|e| {
        errors::Errors::FileOpenError(format!("Failed to zero-fill new WAL segment file: {}", e))
    })?;

    file.seek(std::io::SeekFrom::Start(0))
        .await
        .map_err(|e| errors::Errors::WALSegmentFileOpenError(e.to_string()))?;

    Ok(())
}

pub enum ShutdownType {
    Immediate,
    Graceful,
}

pub async fn handle_shutdown() {
    use tokio::signal::unix;

    let mut sigquit_signal = unix::signal(unix::SignalKind::quit()).unwrap();
    let mut sigterm_signal = unix::signal(unix::SignalKind::terminate()).unwrap();
    let mut sigint_signal = unix::signal(unix::SignalKind::interrupt()).unwrap();

    tokio::select! {
        _ = sigquit_signal.recv() => {
            log::info!("Received SIGQUIT signal");
        }
        _ = sigterm_signal.recv() => {
            log::info!("Received SIGTERM signal");
        }
        _ = sigint_signal.recv() => {
            log::info!("Received SIGINT signal");
        }
    };
}
