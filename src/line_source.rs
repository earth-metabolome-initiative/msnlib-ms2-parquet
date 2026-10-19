use std::collections::VecDeque;

use mascot_rs::error::Result;
use mascot_rs::mascot_generic_format::MGFLineSource;

/// Prefix of the verbatim header copies injected by [`RawHeaderLineSource`].
pub const RAW_PREFIX: &str = "MSNLIB_RAW:";

/// Line source that follows every `KEY=value` header inside an ion block with
/// a verbatim copy `MSNLIB_RAW:KEY=value`.
///
/// mascot-rs moves typed keys (SMILES, FORMULA, PEPMASS, ...) out of the
/// arbitrary metadata and re-renders them. The prefixed copies match none of
/// its typed keys, so they land unchanged in `arbitrary_metadata()`, while the
/// original lines still go through mascot-rs's typed parsing and validation.
pub struct RawHeaderLineSource<S> {
    source: S,
    queued: VecDeque<String>,
    in_block: bool,
}

impl<S> RawHeaderLineSource<S> {
    pub const fn new(source: S) -> Self {
        Self {
            source,
            queued: VecDeque::new(),
            in_block: false,
        }
    }
}

impl<S: MGFLineSource> MGFLineSource for RawHeaderLineSource<S> {
    type Line<'line>
        = String
    where
        Self: 'line;

    fn next_line(&mut self) -> Option<Result<Self::Line<'_>>> {
        if let Some(queued) = self.queued.pop_front() {
            return Some(Ok(queued));
        }

        let line = match self.source.next_line()? {
            Ok(line) => line.as_ref().to_string(),
            Err(error) => return Some(Err(error)),
        };
        match line.trim() {
            "BEGIN IONS" => self.in_block = true,
            "END IONS" => self.in_block = false,
            trimmed if self.in_block && trimmed.contains('=') => {
                self.queued.push_back(format!("{RAW_PREFIX}{trimmed}"));
            }
            _ => {}
        }
        Some(Ok(line))
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use mascot_rs::mascot_generic_format::MGFReader;

    use super::*;

    #[test]
    fn queues_verbatim_copy_after_each_header() {
        let document =
            "BEGIN IONS\nSOURCE_INSTRUMENT=Orbitrap ID-X\nSMILES=CCO\n100.0 1.0\nEND IONS\n";
        let mut source = RawHeaderLineSource::new(MGFReader::new(Cursor::new(document)));
        let mut lines = Vec::new();
        while let Some(line) = source.next_line() {
            lines.push(line.expect("in-memory reads cannot fail"));
        }
        assert_eq!(
            lines,
            [
                "BEGIN IONS",
                "SOURCE_INSTRUMENT=Orbitrap ID-X",
                "MSNLIB_RAW:SOURCE_INSTRUMENT=Orbitrap ID-X",
                "SMILES=CCO",
                "MSNLIB_RAW:SMILES=CCO",
                "100.0 1.0",
                "END IONS",
            ]
        );
    }
}
