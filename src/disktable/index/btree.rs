use std::{collections::HashMap, io::SeekFrom, path::PathBuf, sync::Arc};

use async_recursion::async_recursion;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::{Mutex, RwLock},
};

use crate::{
    config::{TABLES_DIRECTORY, TABLES_INDEX_DIRECTORY},
    disktable::segment::position::TableRecordPosition,
    errors::{self, ErrorCodes},
};

/// 인덱스 세그먼트 파일의 최대 크기 (1GB)
const INDEX_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;

/// BTree 노드의 고정 크기 (8KB)
const NODE_SIZE: usize = 8192;

/// BTree 노드의 타입
#[derive(Debug, Clone, Copy, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub enum BTreeNodeType {
    Internal, // 내부 노드
    Leaf,     // 리프 노드
}

/// BTree 노드의 위치 정보 (파일 내 오프셋)
#[derive(Debug, Clone, Copy, PartialEq, Eq, bincode::Encode, bincode::Decode)]
pub struct BTreeNodePosition {
    pub offset: u64,
}

/// 리프 노드의 엔트리 (키 -> 데이터 위치)
#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct BTreeLeafEntry {
    pub key: String,
    pub position: TableRecordPosition,
}

/// 내부 노드의 엔트리 (키 -> 자식 노드 포인터)
#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct BTreeInternalEntry {
    pub key: String,
    pub child_position: BTreeNodePosition,
}

/// BTree 노드
#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct BTreeNode {
    pub node_type: BTreeNodeType,
    pub parent: Option<BTreeNodePosition>,
    // 리프 노드의 경우
    pub leaf_entries: Vec<BTreeLeafEntry>,
    // 내부 노드의 경우
    pub internal_entries: Vec<BTreeInternalEntry>,
    pub leftmost_child: Option<BTreeNodePosition>,
}

impl BTreeNode {
    pub fn new_leaf() -> Self {
        Self {
            node_type: BTreeNodeType::Leaf,
            parent: None,
            leaf_entries: Vec::new(),
            internal_entries: Vec::new(),
            leftmost_child: None,
        }
    }

    pub fn new_internal() -> Self {
        Self {
            node_type: BTreeNodeType::Internal,
            parent: None,
            leaf_entries: Vec::new(),
            internal_entries: Vec::new(),
            leftmost_child: None,
        }
    }

    pub fn is_leaf(&self) -> bool {
        self.node_type == BTreeNodeType::Leaf
    }

    pub fn is_full(&self, order: u16) -> bool {
        match self.node_type {
            BTreeNodeType::Leaf => self.leaf_entries.len() as u16 >= order - 1,
            BTreeNodeType::Internal => self.internal_entries.len() as u16 >= order - 1,
        }
    }
}

/// BTree 인덱스 메타데이터
#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct BTreeMetadata {
    pub root_position: Option<BTreeNodePosition>,
    pub order: u16,       // BTree의 차수
    pub next_offset: u64, // 다음 노드를 쓸 위치
}

impl Default for BTreeMetadata {
    fn default() -> Self {
        Self {
            root_position: None,
            order: 64, // 기본 차수
            next_offset: 0,
        }
    }
}

/// 파일 기반 BTree 인덱스
#[derive(Debug)]
pub struct BTreeIndex {
    base_path: PathBuf,
    table_name: String,
    metadata: Arc<Mutex<BTreeMetadata>>,
    file_locks: Arc<RwLock<HashMap<u32, Arc<Mutex<File>>>>>,
}

impl BTreeIndex {
    pub fn new(base_path: PathBuf, table_name: String) -> Self {
        Self {
            base_path,
            table_name,
            metadata: Arc::new(Mutex::new(BTreeMetadata::default())),
            file_locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 인덱스 파일 경로 반환 (세그먼트 번호 포함)
    fn index_file_path(&self, segment_number: u32) -> PathBuf {
        let base = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(&self.table_name)
            .join(TABLES_INDEX_DIRECTORY);

        if segment_number == 0 {
            base.join("index.btree")
        } else {
            base.join(format!("index.btree.{}", segment_number))
        }
    }

    /// 논리적 오프셋을 (세그먼트 번호, 세그먼트 내 오프셋)으로 변환
    fn offset_to_segment(&self, logical_offset: u64) -> (u32, u64) {
        let segment_number = (logical_offset / INDEX_SEGMENT_SIZE) as u32;
        let segment_offset = logical_offset % INDEX_SEGMENT_SIZE;
        (segment_number, segment_offset)
    }

    /// 세그먼트 파일 가져오기 (매번 새로 열기 - 캐시 제거)
    async fn get_segment_file(&self, segment_number: u32) -> errors::Result<Arc<Mutex<File>>> {
        let path = self.index_file_path(segment_number);

        // 1. 캐시에 파일 핸들이 이미 있으면 반환
        {
            let file_locks_guard = self.file_locks.read().await;
            if let Some(file_lock) = file_locks_guard.get(&segment_number) {
                return Ok(file_lock.clone());
            }
        }

        let file = if path.exists() {
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .await
                .map_err(|e| {
                    errors::Errors::new(ErrorCodes::FileOpenError).with_message(format!(
                        "Failed to open segment {} file: {}",
                        segment_number, e
                    ))
                })?
        } else {
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)
                .await
                .map_err(|e| {
                    errors::Errors::new(ErrorCodes::FileOpenError).with_message(format!(
                        "Failed to create segment {} file: {}",
                        segment_number, e
                    ))
                })?
        };

        let file_handle = Arc::new(Mutex::new(file));

        let mut file_locks_guard = self.file_locks.write().await;
        file_locks_guard.insert(segment_number, file_handle.clone());

        Ok(file_handle)
    }

    /// 메타데이터 파일 경로 반환
    fn metadata_file_path(&self) -> PathBuf {
        self.base_path
            .join(TABLES_DIRECTORY)
            .join(&self.table_name)
            .join(TABLES_INDEX_DIRECTORY)
            .join("index.metadata")
    }

    /// 인덱스 초기화 (파일 열기 또는 생성)
    pub async fn initialize(&self) -> errors::Result<()> {
        let metadata_path = self.metadata_file_path();

        // 메타데이터 파일 읽기 또는 생성
        if metadata_path.exists() {
            let metadata_bytes = tokio::fs::read(&metadata_path).await.map_err(|e| {
                errors::Errors::new(ErrorCodes::FileReadError)
                    .with_message(format!("Failed to read metadata file: {}", e))
            })?;

            let metadata: BTreeMetadata =
                bincode::decode_from_slice(&metadata_bytes, bincode::config::standard())
                    .map_err(|e| {
                        errors::Errors::new(ErrorCodes::FileReadError)
                            .with_message(format!("Failed to decode metadata: {}", e))
                    })?
                    .0;

            // 인덱스 파일 유효성 검증
            let is_valid = self.validate_index_files(&metadata).await;

            if is_valid {
                let mut meta_guard = self.metadata.lock().await;
                *meta_guard = metadata;
            } else {
                // 손상된 인덱스 파일 정리 및 재생성
                log::warn!(
                    "Index files are corrupted. Reinitializing index for table '{}'",
                    self.table_name
                );
                self.cleanup_index_files().await?;
                self.save_metadata().await?;
            }
        } else {
            // 새로운 메타데이터 생성
            self.save_metadata().await?;
        }

        Ok(())
    }

    /// 인덱스 파일 유효성 검증
    async fn validate_index_files(&self, metadata: &BTreeMetadata) -> bool {
        // next_offset이 0이면 빈 인덱스 (아직 아무것도 안 씀)
        if metadata.next_offset == 0 {
            // root_position이 있으면 모순
            if metadata.root_position.is_some() {
                log::warn!("Metadata inconsistency: root_position exists but next_offset is 0");
                return false;
            }
            return true;
        }

        // root_position이 없으면 빈 인덱스로 간주
        let Some(root_pos) = metadata.root_position else {
            // next_offset이 0이 아닌데 root가 없으면 모순
            log::warn!(
                "Metadata inconsistency: next_offset is {} but no root_position",
                metadata.next_offset
            );
            return false;
        };

        // next_offset으로 마지막 세그먼트 확인
        let (last_segment, _) = self.offset_to_segment(metadata.next_offset);

        // 모든 세그먼트 파일이 존재하는지 확인
        for seg_num in 0..=last_segment {
            let path = self.index_file_path(seg_num);
            if !path.exists() {
                log::warn!("Missing index segment file: {}", path.display());
                return false;
            }
        }

        // 루트 노드가 있는 세그먼트 파일 확인
        let (segment_number, segment_offset) = self.offset_to_segment(root_pos.offset);
        let path = self.index_file_path(segment_number);

        // 파일을 열고 실제로 노드를 읽어보기
        match OpenOptions::new().read(true).open(&path).await {
            Ok(mut file) => {
                // 파일 크기 확인
                let file_size = match file.metadata().await {
                    Ok(meta) => meta.len(),
                    Err(e) => {
                        log::warn!("Failed to get file metadata: {}", e);
                        return false;
                    }
                };

                // 최소한 헤더(4바이트) + 일부 데이터가 있어야 함
                if segment_offset + 4 > file_size {
                    log::warn!(
                        "Index file too small: offset {} + 4 > file size {}",
                        segment_offset,
                        file_size
                    );
                    return false;
                }

                // 노드 크기 헤더 읽기
                if let Err(e) = file.seek(SeekFrom::Start(segment_offset)).await {
                    log::warn!("Failed to seek: {}", e);
                    return false;
                }

                let node_size = match file.read_u32().await {
                    Ok(size) => size,
                    Err(e) => {
                        log::warn!("Failed to read node size: {}", e);
                        return false;
                    }
                };

                // 노드 크기가 비정상적으로 크거나 파일 크기를 초과하는지 확인
                if node_size > 10_000_000 {
                    // 10MB 이상은 비정상
                    log::warn!(
                        "Node size {} is suspiciously large (>10MB). Index is corrupted.",
                        node_size
                    );
                    return false;
                }

                if node_size == 0 {
                    log::warn!("Node size is 0. Index is corrupted.");
                    return false;
                }

                if segment_offset + 4 + node_size as u64 > file_size {
                    log::warn!(
                        "Node size {} exceeds file bounds: offset {} + 4 + {} > file size {}. Index is corrupted.",
                        node_size,
                        segment_offset,
                        node_size,
                        file_size
                    );
                    return false;
                }

                // 실제로 데이터를 읽어서 디코딩 시도
                let mut buffer = vec![0u8; node_size as usize];
                if let Err(e) = file.read_exact(&mut buffer).await {
                    log::warn!("Failed to read node data: {}", e);
                    return false;
                }

                // 디코딩 시도
                let decode_result: Result<(BTreeNode, usize), _> =
                    bincode::decode_from_slice(&buffer, bincode::config::standard());

                match decode_result {
                    Ok(_) => {
                        log::debug!("Index validation passed for table '{}'", self.table_name);
                        true
                    }
                    Err(e) => {
                        log::warn!("Failed to decode node: {}", e);
                        false
                    }
                }
            }
            Err(e) => {
                log::warn!("Failed to open index file {}: {}", path.display(), e);
                false
            }
        }
    }

    /// 손상된 인덱스 파일 정리
    async fn cleanup_index_files(&self) -> errors::Result<()> {
        let index_dir = self
            .base_path
            .join(TABLES_DIRECTORY)
            .join(&self.table_name)
            .join(TABLES_INDEX_DIRECTORY);

        if !index_dir.exists() {
            return Ok(());
        }

        // 인덱스 디렉토리 내 모든 index.btree* 파일 삭제
        let mut entries = tokio::fs::read_dir(&index_dir).await.map_err(|e| {
            errors::Errors::new(ErrorCodes::FileReadError)
                .with_message(format!("Failed to read index directory: {}", e))
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            errors::Errors::new(ErrorCodes::FileReadError)
                .with_message(format!("Failed to read directory entry: {}", e))
        })? {
            let path = entry.path();
            if let Some(file_name) = path.file_name().and_then(|n| n.to_str())
                && file_name.starts_with("index.btree")
            {
                tokio::fs::remove_file(&path).await.map_err(|e| {
                    errors::Errors::new(ErrorCodes::FileWriteError).with_message(format!(
                        "Failed to remove file {}: {}",
                        path.display(),
                        e
                    ))
                })?;
            }
        }

        Ok(())
    }

    /// 메타데이터 저장
    async fn save_metadata(&self) -> errors::Result<()> {
        let metadata_path = self.metadata_file_path();

        // 락을 짧게 잡고 인코딩만 수행
        let encoded = {
            let meta_guard = self.metadata.lock().await;
            bincode::encode_to_vec(&*meta_guard, bincode::config::standard()).map_err(|e| {
                errors::Errors::new(ErrorCodes::FileWriteError)
                    .with_message(format!("Failed to encode metadata: {}", e))
            })?
        }; // 락 해제

        // 디렉터리 생성 보장
        if let Some(parent) = metadata_path.parent()
            && !parent.exists()
        {
            tokio::fs::create_dir_all(parent).await.map_err(|e| {
                errors::Errors::new(ErrorCodes::FileWriteError)
                    .with_message(format!("Failed to create index directory: {}", e))
            })?;
        }

        // 락 없이 파일 쓰기
        tokio::fs::write(&metadata_path, &encoded)
            .await
            .map_err(|e| {
                errors::Errors::new(ErrorCodes::FileWriteError)
                    .with_message(format!("Failed to write metadata file: {}", e))
            })?;

        Ok(())
    }

    /// 노드 읽기
    async fn read_node(&self, position: BTreeNodePosition) -> errors::Result<BTreeNode> {
        // 논리적 오프셋을 세그먼트 정보로 변환
        let (segment_number, segment_offset) = self.offset_to_segment(position.offset);

        // 해당 세그먼트 파일 열기
        let file_handle = self.get_segment_file(segment_number).await?;
        let mut file = file_handle.lock().await;

        // 파일 크기 확인
        let file_size = file
            .metadata()
            .await
            .map_err(|e| {
                errors::Errors::new(ErrorCodes::FileReadError)
                    .with_message(format!("Failed to get file metadata: {}", e))
            })?
            .len();

        if segment_offset + 4 > file_size {
            log::error!(
                "[BTree:{}] Read error: offset {} + 4 > file size {}. Segment: {}, Logical offset: {}",
                self.table_name,
                segment_offset,
                file_size,
                segment_number,
                position.offset
            );
            return Err(errors::Errors::new(ErrorCodes::FileReadError).with_message(format!(
                "Attempt to read beyond file size: offset {} + 4 > file size {}. Index may be corrupted.",
                segment_offset, file_size
            )));
        }

        file.seek(SeekFrom::Start(segment_offset))
            .await
            .map_err(|e| {
                errors::Errors::new(ErrorCodes::FileSeekError)
                    .with_message(format!("Failed to seek to node position: {}", e))
            })?;

        // 노드 크기 읽기
        let node_size = file.read_u32().await.map_err(|e| {
            log::error!(
                "[BTree:{}] Failed to read size header at offset {}: {}",
                self.table_name,
                segment_offset,
                e
            );
            errors::Errors::new(ErrorCodes::FileReadError).with_message(format!(
                "Failed to read node size at offset {}: {}",
                segment_offset, e
            ))
        })?;

        // 노드 크기 검증
        if node_size == 0 {
            log::error!(
                "[BTree:{}] Invalid node size 0 at offset {}. Segment: {}, Logical: {}. This indicates uninitialized or corrupted space.",
                self.table_name,
                segment_offset,
                segment_number,
                position.offset
            );
            return Err(errors::Errors::new(ErrorCodes::FileReadError).with_message(format!(
                "Invalid node size 0 at offset {}. Index may be corrupted or reading uninitialized space.",
                segment_offset
            )));
        }

        let max_data_size = NODE_SIZE - 4;
        if node_size > max_data_size as u32 {
            log::error!(
                "[BTree:{}] Node size {} exceeds maximum {} at offset {}. Segment: {}, Logical: {}",
                self.table_name,
                node_size,
                max_data_size,
                segment_offset,
                segment_number,
                position.offset
            );
            return Err(
                errors::Errors::new(ErrorCodes::FileReadError).with_message(format!(
                    "Node size {} exceeds maximum block size {}. Index may be corrupted.",
                    node_size, max_data_size
                )),
            );
        }

        if segment_offset + 4 + node_size as u64 > file_size {
            log::error!(
                "[BTree:{}] Node size error: offset {} + 4 + {} > file size {}. Segment: {}, Logical: {}",
                self.table_name,
                segment_offset,
                node_size,
                file_size,
                segment_number,
                position.offset
            );
            return Err(errors::Errors::new(ErrorCodes::FileReadError).with_message(format!(
                "Node size {} exceeds file bounds: offset {} + 4 + {} > file size {}. Index may be corrupted.",
                node_size, segment_offset, node_size, file_size
            )));
        }

        // 노드 데이터 읽기
        let mut buffer = vec![0u8; node_size as usize];
        file.read_exact(&mut buffer).await.map_err(|e| {
            log::error!(
                "[BTree:{}] Read exact failed: size={}, offset={}, error={}",
                self.table_name,
                node_size,
                segment_offset,
                e
            );
            errors::Errors::new(ErrorCodes::FileReadError).with_message(format!(
                "Failed to read node data of size {} at offset {}: {}",
                node_size, segment_offset, e
            ))
        })?;

        // 디코딩
        let node: BTreeNode = bincode::decode_from_slice(&buffer, bincode::config::standard())
            .map_err(|e| {
                log::error!(
                    "[BTree:{}] Decode failed at offset {}: {}. Buffer size: {}, Header bytes: {:02X?}",
                    self.table_name, position.offset, e, buffer.len(),
                    &buffer[..buffer.len().min(32)]
                );
                errors::Errors::new(ErrorCodes::FileReadError)
                    .with_message(format!("Failed to decode node: {}", e))
            })?
            .0;

        Ok(node)
    }

    /// 노드 쓰기 (고정 크기 블록 사용)
    async fn write_node(&self, node: &BTreeNode) -> errors::Result<BTreeNodePosition> {
        // 1. 노드 인코딩 (락 없이)
        let encoded = bincode::encode_to_vec(node, bincode::config::standard()).map_err(|e| {
            errors::Errors::new(ErrorCodes::FileWriteError)
                .with_message(format!("Failed to encode node: {}", e))
        })?;

        // 고정 크기 블록 검증
        let max_data_size = NODE_SIZE - 4;
        if encoded.len() > max_data_size {
            log::error!(
                "[BTree:{}] Node too large: {} > {} (type={:?}, entries={})",
                self.table_name,
                encoded.len(),
                max_data_size,
                node.node_type,
                node.leaf_entries.len() + node.internal_entries.len()
            );
            return Err(
                errors::Errors::new(ErrorCodes::FileWriteError).with_message(format!(
                    "Node size {} exceeds maximum block size {}",
                    encoded.len(),
                    max_data_size
                )),
            );
        }

        // 2. 오프셋 예약 (락을 잡고 즉시 증가시켜서 다른 스레드가 같은 offset을 받지 못하게 함)
        let (logical_offset, segment_number, segment_offset) = {
            let mut meta_guard = self.metadata.lock().await;
            let logical_offset = meta_guard.next_offset;

            // 오프셋 즉시 증가 (예약)
            meta_guard.next_offset += NODE_SIZE as u64;

            // 세그먼트 정보 계산
            let (seg_num, seg_off) = self.offset_to_segment(logical_offset);

            // 락 해제
            drop(meta_guard);

            (logical_offset, seg_num, seg_off)
        };

        let position = BTreeNodePosition {
            offset: logical_offset,
        };

        // 3. 파일 I/O 수행 (락으로 보호하여 seek/write가 원자적으로 실행되도록)
        let node_size = encoded.len() as u32;
        let size_bytes = node_size.to_be_bytes();

        // 해당 세그먼트 파일 열기
        let file_handle = self.get_segment_file(segment_number).await?;
        let mut file = file_handle.lock().await;

        file.seek(SeekFrom::Start(segment_offset))
            .await
            .map_err(|e| {
                errors::Errors::new(ErrorCodes::FileSeekError)
                    .with_message(format!("Failed to seek to write position: {}", e))
            })?;

        // 크기 쓰기
        file.write_all(&size_bytes).await.map_err(|e| {
            errors::Errors::new(ErrorCodes::FileWriteError)
                .with_message(format!("Failed to write node size: {}", e))
        })?;

        // 노드 데이터 쓰기
        file.write_all(&encoded).await.map_err(|e| {
            errors::Errors::new(ErrorCodes::FileWriteError)
                .with_message(format!("Failed to write node data: {}", e))
        })?;

        // 나머지 공간을 0으로 패딩 (고정 크기 유지)
        let padding_size = max_data_size - encoded.len();
        if padding_size > 0 {
            let padding = vec![0u8; padding_size];
            file.write_all(&padding).await.map_err(|e| {
                errors::Errors::new(ErrorCodes::FileWriteError)
                    .with_message(format!("Failed to write padding: {}", e))
            })?;
        }

        // 메타데이터 저장 (next_offset은 이미 증가되어 있음)
        self.save_metadata().await?;

        Ok(position)
    }

    /// 노드 업데이트 (기존 위치에 in-place 덮어쓰기)
    async fn update_node(
        &self,
        position: BTreeNodePosition,
        node: &BTreeNode,
    ) -> errors::Result<()> {
        // 논리적 오프셋을 세그먼트 정보로 변환
        let (segment_number, segment_offset) = self.offset_to_segment(position.offset);

        // 노드 인코딩
        let encoded = bincode::encode_to_vec(node, bincode::config::standard()).map_err(|e| {
            errors::Errors::new(ErrorCodes::FileWriteError)
                .with_message(format!("Failed to encode node: {}", e))
        })?;

        // 고정 크기 블록 체크
        let max_data_size = NODE_SIZE - 4;
        if encoded.len() > max_data_size {
            log::error!(
                "[BTree:{}] Update: Node too large: {} > {} at offset={}",
                self.table_name,
                encoded.len(),
                max_data_size,
                position.offset
            );
            return Err(
                errors::Errors::new(ErrorCodes::FileWriteError).with_message(format!(
                    "Node size {} exceeds maximum block size {}",
                    encoded.len(),
                    max_data_size
                )),
            );
        }

        let node_size = encoded.len() as u32;
        let size_bytes = node_size.to_be_bytes();

        // 해당 세그먼트 파일 열기

        // 파일 I/O를 락으로 보호
        let file_handle = self.get_segment_file(segment_number).await?;
        let mut file = file_handle.lock().await;

        file.seek(SeekFrom::Start(segment_offset))
            .await
            .map_err(|e| {
                errors::Errors::new(ErrorCodes::FileSeekError)
                    .with_message(format!("Failed to seek to update position: {}", e))
            })?;

        // 크기 쓰기
        file.write_all(&size_bytes).await.map_err(|e| {
            errors::Errors::new(ErrorCodes::FileWriteError)
                .with_message(format!("Failed to write node size: {}", e))
        })?;

        // 노드 데이터 쓰기
        file.write_all(&encoded).await.map_err(|e| {
            errors::Errors::new(ErrorCodes::FileWriteError)
                .with_message(format!("Failed to write node data: {}", e))
        })?;

        // 나머지 공간을 0으로 패딩
        let padding_size = max_data_size - encoded.len();
        if padding_size > 0 {
            let padding = vec![0u8; padding_size];
            file.write_all(&padding).await.map_err(|e| {
                errors::Errors::new(ErrorCodes::FileWriteError)
                    .with_message(format!("Failed to write padding: {}", e))
            })?;
        }

        Ok(())
    }

    /// 키를 기반으로 레코드 위치 찾기
    pub async fn find(&self, key: &str) -> errors::Result<Option<TableRecordPosition>> {
        let meta_guard = self.metadata.lock().await;
        let root_pos = match meta_guard.root_position {
            Some(pos) => pos,
            None => {
                return Ok(None);
            }
        };
        drop(meta_guard);

        self.find_in_node(root_pos, key).await
    }

    /// 특정 노드에서 키 찾기 (재귀적)
    #[async_recursion]
    async fn find_in_node(
        &self,
        node_pos: BTreeNodePosition,
        key: &str,
    ) -> errors::Result<Option<TableRecordPosition>> {
        let node = self.read_node(node_pos).await?;

        match node.node_type {
            BTreeNodeType::Leaf => {
                // 리프 노드에서 직접 검색
                for entry in &node.leaf_entries {
                    if entry.key == key {
                        return Ok(Some(entry.position.clone()));
                    }
                }
                Ok(None)
            }
            BTreeNodeType::Internal => {
                // 내부 노드는 반드시 leftmost_child를 가져야 함
                if node.leftmost_child.is_none() {
                    return Err(errors::Errors::new(ErrorCodes::FileReadError)
                        .with_message(format!(
                            "Internal node at offset {} has no leftmost_child. Index may be corrupted.",
                            node_pos.offset
                        )));
                }

                // 내부 노드에서 적절한 자식 찾기
                let mut child_pos = node.leftmost_child.unwrap();

                for entry in &node.internal_entries {
                    if key < entry.key.as_str() {
                        break;
                    }
                    child_pos = entry.child_position;
                }

                self.find_in_node(child_pos, key).await
            }
        }
    }

    /// 키-값 삽입
    pub async fn insert(&self, key: String, position: TableRecordPosition) -> errors::Result<()> {
        let meta_guard = self.metadata.lock().await;

        // 루트가 없으면 새로운 리프 노드 생성
        if meta_guard.root_position.is_none() {
            let mut root = BTreeNode::new_leaf();
            root.leaf_entries.push(BTreeLeafEntry { key, position });

            drop(meta_guard);
            let root_pos = self.write_node(&root).await?;

            let mut meta_guard = self.metadata.lock().await;
            meta_guard.root_position = Some(root_pos);
            drop(meta_guard);

            self.save_metadata().await?;
            return Ok(());
        }

        let root_pos = meta_guard.root_position.unwrap();
        let order = meta_guard.order;
        drop(meta_guard);

        // 삽입 수행
        if let Some((split_key, new_node_pos)) = self
            .insert_into_node(root_pos, key, position, order)
            .await?
        {
            // 루트가 split되었으므로 새로운 internal 루트 생성
            let mut new_root = BTreeNode::new_internal();
            new_root.leftmost_child = Some(root_pos);
            new_root.internal_entries.push(BTreeInternalEntry {
                key: split_key,
                child_position: new_node_pos,
            });

            let new_root_pos = self.write_node(&new_root).await?;

            // 자식 노드들의 parent 포인터 갱신
            // 1. 기존 루트(leftmost_child)
            let mut old_root = self.read_node(root_pos).await?;
            old_root.parent = Some(new_root_pos);
            self.update_node(root_pos, &old_root).await?;

            // 2. 분할된 새 노드
            let mut split_node = self.read_node(new_node_pos).await?;
            split_node.parent = Some(new_root_pos);
            self.update_node(new_node_pos, &split_node).await?;

            let mut meta_guard = self.metadata.lock().await;
            meta_guard.root_position = Some(new_root_pos);
            drop(meta_guard);

            self.save_metadata().await?;
        }

        Ok(())
    }

    /// 노드에 삽입 (재귀적)
    #[async_recursion]
    async fn insert_into_node(
        &self,
        node_pos: BTreeNodePosition,
        key: String,
        position: TableRecordPosition,
        order: u16,
    ) -> errors::Result<Option<(String, BTreeNodePosition)>> {
        let mut node = self.read_node(node_pos).await?;

        match node.node_type {
            BTreeNodeType::Leaf => {
                // 리프 노드에 삽입
                let insert_pos = node
                    .leaf_entries
                    .binary_search_by(|entry| entry.key.as_str().cmp(&key))
                    .unwrap_or_else(|pos| pos);

                node.leaf_entries.insert(
                    insert_pos,
                    BTreeLeafEntry {
                        key: key.clone(),
                        position,
                    },
                );

                // 노드가 가득 찼는지 확인
                if node.is_full(order) {
                    self.split_leaf_node(node_pos, node, order).await
                } else {
                    self.update_node(node_pos, &node).await?;
                    Ok(None)
                }
            }
            BTreeNodeType::Internal => {
                // 내부 노드는 반드시 leftmost_child를 가져야 함
                if node.leftmost_child.is_none() {
                    return Err(errors::Errors::new(ErrorCodes::FileReadError)
                        .with_message(format!(
                            "Internal node at offset {} has no leftmost_child. Index may be corrupted.",
                            node_pos.offset
                        )));
                }

                // 적절한 자식 노드 찾기
                let mut child_pos = node.leftmost_child;
                let mut insert_index = 0;

                for (i, entry) in node.internal_entries.iter().enumerate() {
                    if key < entry.key {
                        break;
                    }
                    child_pos = Some(entry.child_position);
                    insert_index = i + 1;
                }

                // child_pos는 위에서 leftmost_child로 초기화되므로 항상 Some
                let pos = child_pos.unwrap();

                // 자식 노드에 재귀적으로 삽입
                if let Some((split_key, new_child_pos)) =
                    self.insert_into_node(pos, key, position, order).await?
                {
                    // 분할된 새 자식의 parent 포인터 갱신
                    let mut new_child = self.read_node(new_child_pos).await?;
                    new_child.parent = Some(node_pos);
                    self.update_node(new_child_pos, &new_child).await?;

                    // 분할된 노드 처리
                    node.internal_entries.insert(
                        insert_index,
                        BTreeInternalEntry {
                            key: split_key,
                            child_position: new_child_pos,
                        },
                    );

                    if node.is_full(order) {
                        self.split_internal_node(node_pos, node, order).await
                    } else {
                        self.update_node(node_pos, &node).await?;
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
        }
    }

    /// 리프 노드 분할
    async fn split_leaf_node(
        &self,
        node_pos: BTreeNodePosition,
        mut node: BTreeNode,
        _order: u16,
    ) -> errors::Result<Option<(String, BTreeNodePosition)>> {
        let mid = node.leaf_entries.len() / 2;
        let split_key = node.leaf_entries[mid].key.clone();

        let mut new_node = BTreeNode::new_leaf();
        new_node.leaf_entries = node.leaf_entries.split_off(mid);
        new_node.parent = node.parent;

        let new_node_pos = self.write_node(&new_node).await?;
        self.update_node(node_pos, &node).await?;

        Ok(Some((split_key, new_node_pos)))
    }

    /// 내부 노드 분할
    async fn split_internal_node(
        &self,
        node_pos: BTreeNodePosition,
        mut node: BTreeNode,
        _order: u16,
    ) -> errors::Result<Option<(String, BTreeNodePosition)>> {
        let mid = node.internal_entries.len() / 2;
        let split_key = node.internal_entries[mid].key.clone();

        let mut new_node = BTreeNode::new_internal();

        // mid+1 이후의 엔트리들을 new_node로 이동
        new_node.internal_entries = node.internal_entries.split_off(mid + 1);

        // mid 위치의 엔트리를 pop하여 new_node의 leftmost_child로 설정
        let mid_entry = node.internal_entries.pop().ok_or_else(|| {
            errors::Errors::new(ErrorCodes::FileReadError).with_message(
                "Internal node split failed: no entry at mid position. This should never happen."
                    .to_string(),
            )
        })?;

        new_node.leftmost_child = Some(mid_entry.child_position);
        new_node.parent = node.parent;

        // CRITICAL: 원본 노드도 leftmost_child를 유지해야 함!
        // node.leftmost_child는 이미 설정되어 있으므로 그대로 유지
        // (변경하지 않음)

        let new_node_pos = self.write_node(&new_node).await?;

        // new_node로 이동한 자식 노드들의 parent 포인터 갱신
        // 1. leftmost_child 갱신
        if let Some(child_pos) = new_node.leftmost_child {
            let mut child = self.read_node(child_pos).await?;
            child.parent = Some(new_node_pos);
            self.update_node(child_pos, &child).await?;
        }

        // 2. internal_entries의 모든 자식들 갱신
        for entry in &new_node.internal_entries {
            let mut child = self.read_node(entry.child_position).await?;
            child.parent = Some(new_node_pos);
            self.update_node(entry.child_position, &child).await?;
        }

        self.update_node(node_pos, &node).await?;

        Ok(Some((split_key, new_node_pos)))
    }

    /// 키 삭제
    pub async fn delete(&self, key: &str) -> errors::Result<()> {
        let meta_guard = self.metadata.lock().await;
        let root_pos = match meta_guard.root_position {
            Some(pos) => pos,
            None => return Ok(()), // 빈 트리
        };
        drop(meta_guard);

        self.delete_from_node(root_pos, key).await?;

        Ok(())
    }

    /// 노드에서 삭제 (재귀적)
    #[async_recursion]
    async fn delete_from_node(
        &self,
        node_pos: BTreeNodePosition,
        key: &str,
    ) -> errors::Result<bool> {
        let mut node = self.read_node(node_pos).await?;

        match node.node_type {
            BTreeNodeType::Leaf => {
                // 리프 노드에서 삭제
                if let Some(pos) = node.leaf_entries.iter().position(|e| e.key == key) {
                    node.leaf_entries.remove(pos);
                    self.update_node(node_pos, &node).await?;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            BTreeNodeType::Internal => {
                // 내부 노드는 반드시 leftmost_child를 가져야 함
                if node.leftmost_child.is_none() {
                    return Err(errors::Errors::new(ErrorCodes::FileReadError)
                        .with_message(format!(
                            "Internal node at offset {} has no leftmost_child. Index may be corrupted.",
                            node_pos.offset
                        )));
                }

                // 적절한 자식 노드 찾기
                let mut child_pos = node.leftmost_child.unwrap();

                for entry in &node.internal_entries {
                    if key < entry.key.as_str() {
                        break;
                    }
                    child_pos = entry.child_position;
                }

                self.delete_from_node(child_pos, key).await
            }
        }
    }

    /// 키 업데이트 (삭제 후 삽입)
    pub async fn update(&self, key: String, position: TableRecordPosition) -> errors::Result<()> {
        self.delete(&key).await?;
        self.insert(key, position).await?;
        Ok(())
    }
}
