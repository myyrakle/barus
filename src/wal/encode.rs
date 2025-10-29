use bincode::config::{Configuration, Fixint, LittleEndian, NoLimit};

use crate::{errors, wal::record::WALRecord};

pub trait WALRecordCodec {
    fn encode(&self, record: &WALRecord, buf: &mut [u8]) -> errors::Result<usize>;
    fn decode(&self, data: &[u8]) -> errors::Result<WALRecord>;
}

pub struct WALRecordBincodeCodec;

impl WALRecordBincodeCodec {
    const CONFIG: Configuration<LittleEndian, Fixint, NoLimit> = bincode::config::standard()
        .with_fixed_int_encoding()
        .with_little_endian()
        .with_no_limit();
}

impl WALRecordCodec for WALRecordBincodeCodec {
    fn encode(&self, record: &WALRecord, buf: &mut [u8]) -> errors::Result<usize> {
        bincode::encode_into_slice(record, buf, Self::CONFIG).map_err(|e| {
            errors::Errors::new(errors::ErrorCodes::WALRecordEncodeError)
                .with_message(e.to_string())
        })
    }

    fn decode(&self, data: &[u8]) -> errors::Result<WALRecord> {
        // bincode 2.x uses decode_from_slice with config
        let (decoded, _len): (WALRecord, usize) = bincode::decode_from_slice(data, Self::CONFIG)
            .map_err(|e| {
                errors::Errors::new(errors::ErrorCodes::WALRecordDecodeError)
                    .with_message(e.to_string())
            })?;
        Ok(decoded)
    }
}
