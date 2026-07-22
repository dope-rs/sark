#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetadataEntry {
    pub name: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Metadata {
    entries: Vec<MetadataEntry>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MetadataError {
    EmptyName,
    UppercaseName,
    ConnectionHeader,
    BadBinaryName,
}

impl Metadata {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    pub fn push(&mut self, name: &[u8], value: &[u8]) -> Result<(), MetadataError> {
        Self::validate_name(name)?;
        self.entries.push(MetadataEntry {
            name: name.to_vec(),
            value: value.to_vec(),
        });
        Ok(())
    }

    pub fn entries(&self) -> &[MetadataEntry] {
        &self.entries
    }

    pub fn get_all<'a>(&'a self, name: &'a [u8]) -> impl Iterator<Item = &'a [u8]> + 'a {
        self.entries
            .iter()
            .filter(move |e| e.name == name)
            .map(|e| e.value.as_slice())
    }

    fn validate_name(name: &[u8]) -> Result<(), MetadataError> {
        if name.is_empty() {
            return Err(MetadataError::EmptyName);
        }
        if name.iter().any(u8::is_ascii_uppercase) {
            return Err(MetadataError::UppercaseName);
        }
        match name {
            b"connection" | b"keep-alive" | b"proxy-connection" | b"transfer-encoding"
            | b"upgrade" => {
                return Err(MetadataError::ConnectionHeader);
            }
            _ => {}
        }
        if name.ends_with(b"-bin") && name.len() == 4 {
            return Err(MetadataError::BadBinaryName);
        }
        Ok(())
    }

    pub fn from_h2_fields(headers: &[OwnedField]) -> Result<Metadata, Status> {
        let mut metadata = Metadata::new();
        for header in headers {
            if header.name.starts_with(b":") || Self::is_reserved(&header.name) {
                continue;
            }
            metadata
                .push(&header.name, &header.value)
                .map_err(Status::from_metadata_err)?;
        }
        Ok(metadata)
    }

    pub(super) fn is_reserved(name: &[u8]) -> bool {
        matches!(
            name,
            b"content-type" | b"te" | b"grpc-status" | b"grpc-message" | b"grpc-status-details-bin"
        )
    }
}
use sark_core::http::OwnedField;

use crate::status::Status;
