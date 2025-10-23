use crate::errors;
use tokio::fs::File;

#[cfg(target_os = "linux")]
pub async fn file_resize_and_set_zero(file: &mut File, size: u64) -> errors::Result<()> {
    use std::os::fd::{AsFd, AsRawFd};

    let fd = file.as_fd().as_raw_fd();

    // let result = unsafe {
    //     libc::fallocate(
    //         fd,
    //         0, // flags = 0 은 공간만 할당 (초기화 안됨)
    //         0,
    //         size as i64,
    //     )
    // };

    // if result != 0 {
    //     use crate::errors;

    //     return Err(errors::Errors::FileOpenError(format!(
    //         "Failed to allocate space for new WAL segment file: {}",
    //         std::io::Error::last_os_error()
    //     )));
    // }

    let result = unsafe {
        libc::fallocate(
            fd,
            libc::FALLOC_FL_ZERO_RANGE, // 0으로 채우기
            0,
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
pub async fn file_resize_and_set_zero(file: &mut File, size: u64) -> Result<(), errors::Errors> {
    use tokio::io::{AsyncSeekExt, AsyncWriteExt};

    file.set_len(size as u64).await.map_err(|e| {
        errors::Errors::WALSegmentFileOpenError(format!(
            "Failed to set length for new WAL segment file: {}",
            e
        ))
    })?;

    let zero_bytes = vec![0; size as usize];
    file.write_all(&zero_bytes).await.map_err(|e| {
        errors::Errors::FileOpenError(format!("Failed to zero-fill new WAL segment file: {}", e))
    })?;

    file.seek(std::io::SeekFrom::Start(0))
        .await
        .map_err(|e| errors::Errors::WALSegmentFileOpenError(e.to_string()))?;

    Ok(())
}
