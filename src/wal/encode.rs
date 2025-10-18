use bincode::config::{Configuration, Fixint, LittleEndian, NoLimit};

use crate::{errors, wal::record::WalRecord};

pub trait WalRecordCodec {
    fn encode(&self, record: &WalRecord, buf: &mut [u8]) -> errors::Result<usize>;
    fn decode(&self, data: &[u8]) -> errors::Result<WalRecord>;
}

pub struct WalRecordBincodeCodec;

impl WalRecordBincodeCodec {
    const CONFIG: Configuration<LittleEndian, Fixint, NoLimit> = bincode::config::standard()
        .with_fixed_int_encoding()
        .with_little_endian()
        .with_no_limit();
}

impl WalRecordCodec for WalRecordBincodeCodec {
    fn encode(&self, record: &WalRecord, buf: &mut [u8]) -> errors::Result<usize> {
        bincode::encode_into_slice(record, buf, Self::CONFIG)
            .map_err(|e| errors::Errors::WalRecordEncodeError(e.to_string()))
    }

    fn decode(&self, data: &[u8]) -> errors::Result<WalRecord> {
        // bincode 2.x uses decode_from_slice with config
        let (decoded, _len): (WalRecord, usize) = bincode::decode_from_slice(data, Self::CONFIG)
            .map_err(|e| errors::Errors::WalRecordDecodeError(e.to_string()))?;
        Ok(decoded)
    }
}
