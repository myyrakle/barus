use std::fmt::Debug;

use bincode::config::{Configuration, Fixint, LittleEndian, NoLimit};

use crate::{disktable::segment::TableRecordPayload, errors};

pub trait TableRecordCodec: Debug {
    fn encode(&self, record: &TableRecordPayload) -> errors::Result<Vec<u8>>;
    fn encode_zero_copy(
        &self,
        record: &TableRecordPayload,
        buf: &mut [u8],
    ) -> errors::Result<usize>;
    fn decode(&self, data: &[u8]) -> errors::Result<TableRecordPayload>;
}

#[derive(Debug)]
pub struct TableRecordBincodeCodec;

impl TableRecordBincodeCodec {
    const CONFIG: Configuration<LittleEndian, Fixint, NoLimit> = bincode::config::standard()
        .with_fixed_int_encoding()
        .with_little_endian()
        .with_no_limit();
}

impl TableRecordCodec for TableRecordBincodeCodec {
    fn encode(&self, record: &TableRecordPayload) -> errors::Result<Vec<u8>> {
        bincode::encode_to_vec(record, Self::CONFIG)
            .map_err(|e| errors::Errors::TableRecordEncodeError(e.to_string()))
    }

    fn encode_zero_copy(
        &self,
        record: &TableRecordPayload,
        buf: &mut [u8],
    ) -> errors::Result<usize> {
        bincode::encode_into_slice(record, buf, Self::CONFIG)
            .map_err(|e| errors::Errors::TableRecordEncodeError(e.to_string()))
    }

    fn decode(&self, data: &[u8]) -> errors::Result<TableRecordPayload> {
        // bincode 2.x uses decode_from_slice with config
        let (decoded, _len): (TableRecordPayload, usize) =
            bincode::decode_from_slice(data, Self::CONFIG)
                .map_err(|e| errors::Errors::TableRecordDecodeError(e.to_string()))?;

        Ok(decoded)
    }
}
