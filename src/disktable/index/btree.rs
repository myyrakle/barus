use std::{collections::HashMap, io::SeekFrom, path::PathBuf, sync::Arc};

use async_recursion::async_recursion;
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

use crate::{config::TABLES_DIRECTORY, disktable::segment::TableRecordPosition, errors::Errors};

/// 인덱스 세그먼트 파일의 최대 크기 (1GB)
const INDEX_SEGMENT_SIZE: u64 = 1024 * 1024 * 1024;

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

    pub fn is_full(&self, order: usize) -> bool {
        match self.node_type {
            BTreeNodeType::Leaf => self.leaf_entries.len() >= order - 1,
            BTreeNodeType::Internal => self.internal_entries.len() >= order - 1,
        }
    }
}

/// BTree 인덱스 메타데이터
#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct BTreeMetadata {
    pub root_position: Option<BTreeNodePosition>,
    pub order: usize,     // BTree의 차수
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
    // 세그먼트 번호 -> 파일 핸들 캐시
    segment_files: Arc<Mutex<HashMap<u32, File>>>,
}

impl BTreeIndex {
    pub fn new(base_path: PathBuf, table_name: String) -> Self {
        Self {
            base_path,
            table_name,
            metadata: Arc::new(Mutex::new(BTreeMetadata::default())),
            segment_files: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// 인덱스 파일 경로 반환 (세그먼트 번호 포함)
    fn index_file_path(&self, segment_number: u32) -> PathBuf {
        let base = self.base_path.join(TABLES_DIRECTORY).join(&self.table_name);

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

    /// 세그먼트 파일 가져오기 (캐시된 파일 또는 새로 열기)
    async fn get_segment_file(&self, segment_number: u32) -> Result<File, Errors> {
        let mut files = self.segment_files.lock().await;

        // 캐시에 있으면 복제해서 반환
        if files.contains_key(&segment_number) {
            let path = self.index_file_path(segment_number);
            return OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .await
                .map_err(|e| {
                    Errors::FileOpenError(format!(
                        "Failed to open segment {} file: {}",
                        segment_number, e
                    ))
                });
        }

        // 파일 열기 또는 생성
        let path = self.index_file_path(segment_number);
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| {
                Errors::FileOpenError(format!(
                    "Failed to open segment {} file: {}",
                    segment_number, e
                ))
            })?;

        // 캐시에 추가
        let cached_file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .await
            .map_err(|e| {
                Errors::FileOpenError(format!(
                    "Failed to cache segment {} file: {}",
                    segment_number, e
                ))
            })?;

        files.insert(segment_number, cached_file);
        Ok(file)
    }

    /// 메타데이터 파일 경로 반환
    fn metadata_file_path(&self) -> PathBuf {
        self.base_path
            .join(TABLES_DIRECTORY)
            .join(&self.table_name)
            .join("index.metadata")
    }

    /// 인덱스 초기화 (파일 열기 또는 생성)
    pub async fn initialize(&self) -> Result<(), Errors> {
        let metadata_path = self.metadata_file_path();

        // 메타데이터 파일 읽기 또는 생성
        if metadata_path.exists() {
            let metadata_bytes = tokio::fs::read(&metadata_path).await.map_err(|e| {
                Errors::FileReadError(format!("Failed to read metadata file: {}", e))
            })?;

            let metadata: BTreeMetadata =
                bincode::decode_from_slice(&metadata_bytes, bincode::config::standard())
                    .map_err(|e| {
                        Errors::FileReadError(format!("Failed to decode metadata: {}", e))
                    })?
                    .0;

            let mut meta_guard = self.metadata.lock().await;
            *meta_guard = metadata;
        } else {
            // 새로운 메타데이터 생성
            self.save_metadata().await?;
        }

        // 첫 번째 세그먼트 파일만 미리 열기
        self.get_segment_file(0).await?;

        Ok(())
    }

    /// 메타데이터 저장
    async fn save_metadata(&self) -> Result<(), Errors> {
        let metadata_path = self.metadata_file_path();
        let meta_guard = self.metadata.lock().await;

        let encoded = bincode::encode_to_vec(&*meta_guard, bincode::config::standard())
            .map_err(|e| Errors::FileWriteError(format!("Failed to encode metadata: {}", e)))?;

        tokio::fs::write(&metadata_path, encoded)
            .await
            .map_err(|e| Errors::FileWriteError(format!("Failed to write metadata file: {}", e)))?;

        Ok(())
    }

    /// 노드 읽기
    async fn read_node(&self, position: BTreeNodePosition) -> Result<BTreeNode, Errors> {
        // 논리적 오프셋을 세그먼트 정보로 변환
        let (segment_number, segment_offset) = self.offset_to_segment(position.offset);

        // 해당 세그먼트 파일 열기
        let mut file = self.get_segment_file(segment_number).await?;

        file.seek(SeekFrom::Start(segment_offset))
            .await
            .map_err(|e| {
                Errors::FileSeekError(format!("Failed to seek to node position: {}", e))
            })?;

        // 노드 크기 읽기
        let node_size = file
            .read_u32()
            .await
            .map_err(|e| Errors::FileReadError(format!("Failed to read node size: {}", e)))?;

        // 노드 데이터 읽기
        let mut buffer = vec![0u8; node_size as usize];
        file.read_exact(&mut buffer)
            .await
            .map_err(|e| Errors::FileReadError(format!("Failed to read node data: {}", e)))?;

        // 디코딩
        let node: BTreeNode = bincode::decode_from_slice(&buffer, bincode::config::standard())
            .map_err(|e| Errors::FileReadError(format!("Failed to decode node: {}", e)))?
            .0;

        Ok(node)
    }

    /// 노드 쓰기
    async fn write_node(&self, node: &BTreeNode) -> Result<BTreeNodePosition, Errors> {
        let mut meta_guard = self.metadata.lock().await;
        let logical_offset = meta_guard.next_offset;
        let position = BTreeNodePosition {
            offset: logical_offset,
        };

        // 논리적 오프셋을 세그먼트 정보로 변환
        let (segment_number, segment_offset) = self.offset_to_segment(logical_offset);

        // 노드 인코딩
        let encoded = bincode::encode_to_vec(node, bincode::config::standard())
            .map_err(|e| Errors::FileWriteError(format!("Failed to encode node: {}", e)))?;

        let node_size = encoded.len() as u32;
        let size_bytes = node_size.to_le_bytes();

        // 해당 세그먼트 파일 열기
        let mut file = self.get_segment_file(segment_number).await?;

        file.seek(SeekFrom::Start(segment_offset))
            .await
            .map_err(|e| {
                Errors::FileSeekError(format!("Failed to seek to write position: {}", e))
            })?;

        // 크기 쓰기
        file.write_all(&size_bytes)
            .await
            .map_err(|e| Errors::FileWriteError(format!("Failed to write node size: {}", e)))?;

        // 노드 데이터 쓰기
        file.write_all(&encoded)
            .await
            .map_err(|e| Errors::FileWriteError(format!("Failed to write node data: {}", e)))?;

        // 오프셋 업데이트
        meta_guard.next_offset += 4 + encoded.len() as u64;

        drop(meta_guard);

        self.save_metadata().await?;

        Ok(position)
    }

    /// 노드 업데이트 (기존 위치에 덮어쓰기)
    async fn update_node(
        &self,
        position: BTreeNodePosition,
        node: &BTreeNode,
    ) -> Result<(), Errors> {
        // 논리적 오프셋을 세그먼트 정보로 변환
        let (segment_number, segment_offset) = self.offset_to_segment(position.offset);

        // 노드 인코딩
        let encoded = bincode::encode_to_vec(node, bincode::config::standard())
            .map_err(|e| Errors::FileWriteError(format!("Failed to encode node: {}", e)))?;

        let node_size = encoded.len() as u32;
        let size_bytes = node_size.to_le_bytes();

        // 해당 세그먼트 파일 열기
        let mut file = self.get_segment_file(segment_number).await?;

        file.seek(SeekFrom::Start(segment_offset))
            .await
            .map_err(|e| {
                Errors::FileSeekError(format!("Failed to seek to update position: {}", e))
            })?;

        // 크기 쓰기
        file.write_all(&size_bytes)
            .await
            .map_err(|e| Errors::FileWriteError(format!("Failed to write node size: {}", e)))?;

        // 노드 데이터 쓰기
        file.write_all(&encoded)
            .await
            .map_err(|e| Errors::FileWriteError(format!("Failed to write node data: {}", e)))?;

        Ok(())
    }

    /// 키를 기반으로 레코드 위치 찾기
    pub async fn find(&self, key: &str) -> Result<Option<TableRecordPosition>, Errors> {
        let meta_guard = self.metadata.lock().await;
        let root_pos = match meta_guard.root_position {
            Some(pos) => pos,
            None => return Ok(None), // 빈 트리
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
    ) -> Result<Option<TableRecordPosition>, Errors> {
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
                // 내부 노드에서 적절한 자식 찾기
                let mut child_pos = node.leftmost_child;

                for entry in &node.internal_entries {
                    if key < entry.key.as_str() {
                        break;
                    }
                    child_pos = Some(entry.child_position);
                }

                match child_pos {
                    Some(pos) => self.find_in_node(pos, key).await,
                    None => Ok(None),
                }
            }
        }
    }

    /// 키-값 삽입
    pub async fn insert(&self, key: String, position: TableRecordPosition) -> Result<(), Errors> {
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
        self.insert_into_node(root_pos, key, position, order)
            .await?;

        Ok(())
    }

    /// 노드에 삽입 (재귀적)
    #[async_recursion]
    async fn insert_into_node(
        &self,
        node_pos: BTreeNodePosition,
        key: String,
        position: TableRecordPosition,
        order: usize,
    ) -> Result<Option<(String, BTreeNodePosition)>, Errors> {
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

                if let Some(pos) = child_pos {
                    // 자식 노드에 재귀적으로 삽입
                    if let Some((split_key, new_child_pos)) =
                        self.insert_into_node(pos, key, position, order).await?
                    {
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
                } else {
                    Err(Errors::FileReadError("No child position found".to_string()))
                }
            }
        }
    }

    /// 리프 노드 분할
    async fn split_leaf_node(
        &self,
        node_pos: BTreeNodePosition,
        mut node: BTreeNode,
        _order: usize,
    ) -> Result<Option<(String, BTreeNodePosition)>, Errors> {
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
        _order: usize,
    ) -> Result<Option<(String, BTreeNodePosition)>, Errors> {
        let mid = node.internal_entries.len() / 2;
        let split_key = node.internal_entries[mid].key.clone();

        let mut new_node = BTreeNode::new_internal();
        new_node.internal_entries = node.internal_entries.split_off(mid + 1);

        if let Some(entry) = node.internal_entries.pop() {
            new_node.leftmost_child = Some(entry.child_position);
        }

        new_node.parent = node.parent;

        let new_node_pos = self.write_node(&new_node).await?;
        self.update_node(node_pos, &node).await?;

        Ok(Some((split_key, new_node_pos)))
    }

    /// 키 삭제
    pub async fn delete(&self, key: &str) -> Result<(), Errors> {
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
    ) -> Result<bool, Errors> {
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
                // 적절한 자식 노드 찾기
                let mut child_pos = node.leftmost_child;

                for entry in &node.internal_entries {
                    if key < entry.key.as_str() {
                        break;
                    }
                    child_pos = Some(entry.child_position);
                }

                match child_pos {
                    Some(pos) => self.delete_from_node(pos, key).await,
                    None => Ok(false),
                }
            }
        }
    }

    /// 키 업데이트 (삭제 후 삽입)
    pub async fn update(&self, key: String, position: TableRecordPosition) -> Result<(), Errors> {
        self.delete(&key).await?;
        self.insert(key, position).await?;
        Ok(())
    }
}
