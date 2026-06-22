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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_duplicate_metadata() {
        let mut md = Metadata::new();
        md.push(b"x-tag", b"a").unwrap();
        md.push(b"x-tag", b"b").unwrap();

        let values: Vec<&[u8]> = md.get_all(b"x-tag").collect();
        assert_eq!(values, vec![b"a".as_slice(), b"b".as_slice()]);
    }

    #[test]
    fn rejects_uppercase_names() {
        let mut md = Metadata::new();
        assert_eq!(md.push(b"X-Tag", b"a"), Err(MetadataError::UppercaseName));
    }
}
