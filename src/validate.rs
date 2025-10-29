use crate::{
    config::{KEY_BYTES_MAX_SIZE, TABLE_NAME_MAX_SIZE},
    errors,
};

pub fn validate_table_name(table: &str) -> errors::Result<()> {
    // 1. Empty String Check
    if table.is_empty() {
        return Err(errors::Errors::new(errors::ErrorCodes::TableNameIsEmpty));
    }

    // 2. Max Length Check
    if table.len() > TABLE_NAME_MAX_SIZE {
        return Err(errors::Errors::new(errors::ErrorCodes::TableNameTooLong));
    }

    // 3. All Characters are alphanumeric or underscore
    if !table.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Err(errors::Errors::new(errors::ErrorCodes::TableNameIsInvalid)
            .with_message(table.to_string()));
    }

    Ok(())
}

pub fn validate_key(key: &str) -> errors::Result<()> {
    if key.is_empty() {
        return Err(errors::Errors::new(errors::ErrorCodes::KeyIsEmpty));
    }

    if key.len() > KEY_BYTES_MAX_SIZE {
        return Err(errors::Errors::new(errors::ErrorCodes::KeySizeTooLarge));
    }

    Ok(())
}

pub fn validate_value(value: &str) -> errors::Result<()> {
    if value.len() > crate::config::VALUE_BYTES_MAX_SIZE {
        return Err(errors::Errors::new(errors::ErrorCodes::ValueSizeTooLarge));
    }

    Ok(())
}
