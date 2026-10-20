use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, anyhow, bail};
use arrow_array::builder::{Float64Builder, Int64Builder, StringBuilder};
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use mascot_rs::mascot_generic_format::{MGFIter, MGFReader};
use mascot_rs::prelude::{MascotGenericFormat, Spectrum};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::MS2_SUFFIX;
use crate::line_source::{RAW_PREFIX, RawHeaderLineSource};

/// Rows buffered in Arrow builders before they are handed to the Parquet writer.
const BATCH_ROWS: usize = 10_000;
/// Maximum rows per Parquet row group; bounds writer memory (~3 KB/row).
const ROW_GROUP_ROWS: usize = 50_000;

/// MGF keys mapped onto the MassSpecGym columns, in output order.
const MAPPED_RAW_KEYS: [(&str, &str); 7] = [
    ("smiles", "SMILES"),
    ("inchikey", "INCHIAUX"),
    ("formula", "FORMULA"),
    ("parent_mass", "EXACTMASS"),
    ("precursor_mz", "PEPMASS"),
    ("adduct", "ADDUCT"),
    ("instrument_type", "INSTRUMENT_TYPE"),
];
const COLLISION_ENERGY_KEY: &str = "COLLISION_ENERGY";
/// Describes the original peak list, which can disagree with `mzs` after
/// mascot-rs drops zero-intensity peaks and merges duplicate m/z values.
const NUM_PEAKS_KEY: &str = "Num peaks";
/// Written in lowercase: MSnLib releases spell it `Positive` or `POSITIVE`.
const IONMODE_KEY: &str = "IONMODE";
const CHARGE_KEY: &str = "CHARGE";
/// Identifies a record in error messages.
const SCANS_KEY: &str = "SCANS";

/// Per-file parsing outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileStats {
    pub file: String,
    pub accepted: usize,
    pub skipped: usize,
}

enum ColumnSource {
    Identifier,
    Mzs,
    Intensities,
    /// Lookup key (`MSNLIB_RAW:COLLISION_ENERGY`) of the verbatim header copy.
    CollisionEnergy(String),
    /// Lookup key (`MSNLIB_RAW:KEY`) of the verbatim header copy.
    Raw(String),
    /// Lookup key (`MSNLIB_RAW:KEY`) of a verbatim header copy written in lowercase.
    Lowercase(String),
    SourceFile,
}

/// Narrowest type that holds every value of a column; `Int < Float < Text`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum ValueKind {
    Int,
    Float,
    Text,
}

impl ValueKind {
    /// Kind of one MGF header value; `None` for an empty value (written as null).
    fn of(value: &str) -> Option<Self> {
        if value.is_empty() {
            None
        } else if parse_int(value).is_some() {
            Some(Self::Int)
        } else if parse_float(value).is_some() {
            Some(Self::Float)
        } else {
            Some(Self::Text)
        }
    }

    const fn data_type(self) -> DataType {
        match self {
            Self::Int => DataType::Int64,
            Self::Float => DataType::Float64,
            Self::Text => DataType::Utf8,
        }
    }
}

/// Header key -> widest kind among its non-empty values (`None`: only empty values).
type KeyKinds = BTreeMap<String, Option<ValueKind>>;

struct Column {
    name: String,
    source: ColumnSource,
    kind: ValueKind,
}

impl Column {
    fn text(name: impl Into<String>, source: ColumnSource) -> Self {
        Self {
            name: name.into(),
            source,
            kind: ValueKind::Text,
        }
    }

    /// Column backed by the MGF header `key`, typed by the widest kind of its values.
    fn header(
        name: impl Into<String>,
        key: &str,
        key_kinds: &KeyKinds,
        source: fn(String) -> ColumnSource,
    ) -> Self {
        Self {
            name: name.into(),
            source: source(format!("{RAW_PREFIX}{key}")),
            kind: key_kinds
                .get(key)
                .copied()
                .flatten()
                .unwrap_or(ValueKind::Text),
        }
    }

    const fn nullable(&self) -> bool {
        !matches!(
            self.source,
            ColumnSource::Identifier
                | ColumnSource::Mzs
                | ColumnSource::Intensities
                | ColumnSource::SourceFile
        )
    }
}

/// Parses an MGF header value as an integer.
fn parse_int(value: &str) -> Option<i64> {
    value.trim().parse::<i64>().ok()
}

/// Fails unless the written `ionmode` and `charge` agree in polarity:
/// `negative` requires a charge < 0 and `positive` a charge > 0. Records with
/// another or no ion mode are not checked.
fn check_polarity(ionmode: Option<&str>, charge: Option<&str>) -> anyhow::Result<()> {
    let Some(ionmode) = ionmode else {
        return Ok(());
    };
    let (polarity, sign) = if ionmode.eq_ignore_ascii_case("negative") {
        ("negative", -1)
    } else if ionmode.eq_ignore_ascii_case("positive") {
        ("positive", 1)
    } else {
        return Ok(());
    };
    if charge.and_then(parse_int).map(i64::signum) != Some(sign) {
        bail!(
            "ionmode {polarity} requires a {polarity} charge, got {}",
            charge.unwrap_or("none")
        );
    }
    Ok(())
}

/// Parses an MGF header value as a finite float.
fn parse_float(value: &str) -> Option<f64> {
    value
        .trim()
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
}

/// Builds every `*ms2.mgf` file in `input_dir` into the Parquet file `output`.
pub fn run(input_dir: &Path, output: &Path) -> anyhow::Result<()> {
    let mut inputs = Vec::new();
    for entry in std::fs::read_dir(input_dir)
        .with_context(|| format!("cannot read {}", input_dir.display()))?
    {
        let path = entry?.path();
        if path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(MS2_SUFFIX))
        {
            inputs.push(path);
        }
    }
    inputs.sort_by(|a, b| a.file_name().cmp(&b.file_name()));
    if inputs.is_empty() {
        bail!(
            "no *{MS2_SUFFIX} files in {}; run `msnlib-ms2-parquet download` first",
            input_dir.display()
        );
    }

    let stats = build_dataset(&inputs, output)?;
    let (mut accepted, mut skipped) = (0, 0);
    for file in &stats {
        eprintln!(
            "{}: accepted {}, skipped {}",
            file.file, file.accepted, file.skipped
        );
        accepted += file.accepted;
        skipped += file.skipped;
    }
    eprintln!(
        "total: accepted {accepted}, skipped {skipped} in {} files",
        stats.len()
    );
    eprintln!("wrote {}", output.display());
    Ok(())
}

/// Parses `inputs` in order and writes one row per accepted record to `output`.
///
/// Runs two passes: the first collects the header keys and their value types
/// that define the schema and checks every record with [`check_polarity`],
/// the second writes the rows.
pub fn build_dataset(inputs: &[PathBuf], output: &Path) -> anyhow::Result<Vec<FileStats>> {
    let [ionmode_key, charge_key, scans_key] =
        [IONMODE_KEY, CHARGE_KEY, SCANS_KEY].map(|key| format!("{RAW_PREFIX}{key}"));
    let mut key_kinds = KeyKinds::new();
    let mut stats = Vec::with_capacity(inputs.len());
    for path in inputs {
        stats.push(for_each_record(path, |record| {
            let metadata = record.metadata();
            let raw = |key: &str| {
                metadata
                    .arbitrary_metadata_value(key)
                    .filter(|value| !value.is_empty())
            };
            check_polarity(raw(&ionmode_key), raw(&charge_key)).with_context(|| {
                format!(
                    "{}: record with SCANS={}",
                    path.display(),
                    raw(&scans_key).unwrap_or("none")
                )
            })?;
            for (key, value) in metadata.arbitrary_metadata() {
                let Some(raw_key) = key.strip_prefix(RAW_PREFIX) else {
                    continue;
                };
                let kind = ValueKind::of(value);
                if let Some(widest) = key_kinds.get_mut(raw_key) {
                    *widest = (*widest).max(kind);
                } else {
                    key_kinds.insert(raw_key.to_string(), kind);
                }
            }
            Ok(())
        })?);
    }

    let columns = columns(&key_kinds)?;
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let tmp = output.with_extension("parquet.tmp");
    let mut writer = DatasetWriter::create(columns, &tmp)?;
    for (path, expected) in inputs.iter().zip(&stats) {
        let observed = for_each_record(path, |record| writer.push(record, &expected.file))?;
        if observed != *expected {
            bail!("{} changed between passes", expected.file);
        }
    }
    writer.finish()?;
    std::fs::rename(&tmp, output)?;
    Ok(stats)
}

/// Calls `f` on every record of `path` that passes mascot-rs validation.
fn for_each_record(
    path: &Path,
    mut f: impl FnMut(&MascotGenericFormat<f64>) -> anyhow::Result<()>,
) -> anyhow::Result<FileStats> {
    let file = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("{} has no UTF-8 file name", path.display()))?
        .to_string();
    let reader = BufReader::with_capacity(
        1 << 20,
        File::open(path).with_context(|| format!("cannot open {}", path.display()))?,
    );
    let mut iter =
        MGFIter::<f64, _>::from_line_source(RawHeaderLineSource::new(MGFReader::new(reader)))
            .skipping_invalid_records();
    let mut accepted = 0;
    for record in iter.by_ref() {
        f(&record.with_context(|| format!("cannot read {}", path.display()))?)?;
        accepted += 1;
    }
    Ok(FileStats {
        file,
        accepted,
        skipped: iter.skipped_records(),
    })
}

/// Output columns: the MassSpecGym columns, then every other header key, then
/// `source_file`.
fn columns(key_kinds: &KeyKinds) -> anyhow::Result<Vec<Column>> {
    let mut columns = vec![
        Column::text("identifier", ColumnSource::Identifier),
        Column::text("mzs", ColumnSource::Mzs),
        Column::text("intensities", ColumnSource::Intensities),
    ];
    for (name, key) in MAPPED_RAW_KEYS {
        columns.push(Column::header(name, key, key_kinds, ColumnSource::Raw));
    }
    // A numeric COLLISION_ENERGY key holds scalars only, which normalize to themselves.
    columns.push(Column::header(
        "collision_energy",
        COLLISION_ENERGY_KEY,
        key_kinds,
        ColumnSource::CollisionEnergy,
    ));

    let mut rest: Vec<Column> = key_kinds
        .keys()
        .filter(|key| {
            let key = key.as_str();
            key != COLLISION_ENERGY_KEY
                && key != NUM_PEAKS_KEY
                && !MAPPED_RAW_KEYS.iter().any(|(_, mapped)| *mapped == key)
        })
        .map(|key| {
            Column::header(
                key.to_ascii_lowercase().replace(' ', "_"),
                key,
                key_kinds,
                if key == IONMODE_KEY {
                    ColumnSource::Lowercase
                } else {
                    ColumnSource::Raw
                },
            )
        })
        .collect();
    rest.sort_by(|a, b| a.name.cmp(&b.name));
    columns.extend(rest);
    columns.push(Column::text("source_file", ColumnSource::SourceFile));

    let mut names = BTreeSet::new();
    for column in &columns {
        if !names.insert(column.name.as_str()) {
            bail!("column name collision: {}", column.name);
        }
    }
    Ok(columns)
}

/// Strips list brackets and whitespace: `"[20.0, 15.0, 60.0]"` → `"20.0,15.0,60.0"`.
pub fn normalize_collision_energy(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let raw = raw
        .strip_prefix('[')
        .and_then(|inner| inner.strip_suffix(']'))
        .unwrap_or(raw);
    let values: Vec<&str> = raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .collect();
    (!values.is_empty()).then(|| values.join(","))
}

enum ColumnBuilder {
    Utf8(StringBuilder),
    Int64(Int64Builder),
    Float64(Float64Builder),
}

impl ColumnBuilder {
    fn new(column: &Column) -> Self {
        match column.kind {
            ValueKind::Int => Self::Int64(Int64Builder::new()),
            ValueKind::Float => Self::Float64(Float64Builder::new()),
            ValueKind::Text => Self::Utf8(StringBuilder::new()),
        }
    }

    /// Appends `value` (`None` = null), parsing it for numeric columns.
    fn append(&mut self, column: &str, value: Option<&str>) -> anyhow::Result<()> {
        let invalid = |value: &str, kind: &str| anyhow!("{column}: {value:?} is not {kind}");
        match self {
            Self::Utf8(builder) => builder.append_option(value),
            Self::Int64(builder) => builder.append_option(
                value
                    .map(|value| parse_int(value).ok_or_else(|| invalid(value, "an integer")))
                    .transpose()?,
            ),
            Self::Float64(builder) => builder.append_option(
                value
                    .map(|value| parse_float(value).ok_or_else(|| invalid(value, "a float")))
                    .transpose()?,
            ),
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            Self::Utf8(builder) => Arc::new(builder.finish()),
            Self::Int64(builder) => Arc::new(builder.finish()),
            Self::Float64(builder) => Arc::new(builder.finish()),
        }
    }
}

struct DatasetWriter {
    columns: Vec<Column>,
    schema: SchemaRef,
    builders: Vec<ColumnBuilder>,
    writer: ArrowWriter<File>,
    buffered_rows: usize,
    rows: usize,
    scratch: String,
    /// Scratch space for [`round_significant`].
    number: String,
}

impl DatasetWriter {
    fn create(columns: Vec<Column>, path: &Path) -> anyhow::Result<Self> {
        let schema: SchemaRef = Arc::new(Schema::new(
            columns
                .iter()
                .map(|column| Field::new(&column.name, column.kind.data_type(), column.nullable()))
                .collect::<Vec<_>>(),
        ));
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .build();
        let file =
            File::create(path).with_context(|| format!("cannot create {}", path.display()))?;
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))?;
        let builders = columns.iter().map(ColumnBuilder::new).collect();
        Ok(Self {
            columns,
            schema,
            builders,
            writer,
            buffered_rows: 0,
            rows: 0,
            scratch: String::new(),
            number: String::new(),
        })
    }

    fn push(&mut self, record: &MascotGenericFormat<f64>, source_file: &str) -> anyhow::Result<()> {
        self.rows += 1;
        let metadata = record.metadata();
        let raw = |key: &str| {
            metadata
                .arbitrary_metadata_value(key)
                .filter(|value| !value.is_empty())
        };
        for (column, builder) in self.columns.iter().zip(&mut self.builders) {
            let name = column.name.as_str();
            match &column.source {
                ColumnSource::Identifier => {
                    self.scratch.clear();
                    write!(self.scratch, "MSnLibID{:07}", self.rows)?;
                    builder.append(name, Some(&self.scratch))?;
                }
                ColumnSource::Mzs => {
                    join_floats(&mut self.scratch, record.mz())?;
                    builder.append(name, Some(&self.scratch))?;
                }
                ColumnSource::Intensities => {
                    // mascot-rs rejects empty spectra and drops intensities <= 0,
                    // so the maximum is strictly positive.
                    let max = record.intensities().fold(f64::MIN, f64::max);
                    let number = &mut self.number;
                    join_floats(
                        &mut self.scratch,
                        record
                            .intensities()
                            .map(|intensity| round_significant(intensity / max, number)),
                    )?;
                    builder.append(name, Some(&self.scratch))?;
                }
                ColumnSource::CollisionEnergy(key) => {
                    let value = raw(key).and_then(normalize_collision_energy);
                    builder.append(name, value.as_deref())?;
                }
                ColumnSource::Raw(key) => builder.append(name, raw(key))?,
                ColumnSource::Lowercase(key) => match raw(key) {
                    Some(value) => {
                        self.scratch.clear();
                        self.scratch.push_str(value);
                        self.scratch.make_ascii_lowercase();
                        builder.append(name, Some(&self.scratch))?;
                    }
                    None => builder.append(name, None)?,
                },
                ColumnSource::SourceFile => builder.append(name, Some(source_file))?,
            }
        }
        self.buffered_rows += 1;
        if self.buffered_rows == BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        if self.buffered_rows == 0 {
            return Ok(());
        }
        let arrays: Vec<ArrayRef> = self
            .builders
            .iter_mut()
            .map(ColumnBuilder::finish)
            .collect();
        self.writer
            .write(&RecordBatch::try_new(self.schema.clone(), arrays)?)?;
        self.buffered_rows = 0;
        Ok(())
    }

    fn finish(mut self) -> anyhow::Result<()> {
        self.flush()?;
        self.writer.close()?;
        Ok(())
    }
}

/// Significant digits kept in normalized intensities: the most an `f64` holds
/// reliably, so rounding never changes a value by more than its precision.
const INTENSITY_DIGITS: usize = 15;

/// Rounds `value` to [`INTENSITY_DIGITS`] significant digits, dropping the
/// binary noise of the normalizing division: `0.257 / 100.0` then prints as
/// `0.00257` instead of `0.0025700000000000002`. `buffer` is scratch space.
fn round_significant(value: f64, buffer: &mut String) -> f64 {
    buffer.clear();
    write!(buffer, "{value:.*e}", INTENSITY_DIGITS - 1).expect("writing to a String cannot fail");
    buffer.parse().expect("a formatted finite f64 parses back")
}

/// Writes `values` into `out` as comma-separated `{:?}` floats (`1.0`, `40.018979`).
fn join_floats(out: &mut String, values: impl Iterator<Item = f64>) -> std::fmt::Result {
    out.clear();
    for (index, value) in values.enumerate() {
        if index > 0 {
            out.push(',');
        }
        write!(out, "{value:?}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;

    const RECORD: &str = include_str!("../tests/data/msnlib_record.mgf");

    #[test]
    fn collision_energy() {
        assert_eq!(
            normalize_collision_energy("[20.0, 15.0, 60.0]").as_deref(),
            Some("20.0,15.0,60.0")
        );
        assert_eq!(normalize_collision_energy("60.0").as_deref(), Some("60.0"));
        assert_eq!(
            normalize_collision_energy("[20.0, 45.0, 60.0, 40.0, 15.0, 30.0]").as_deref(),
            Some("20.0,45.0,60.0,40.0,15.0,30.0")
        );
        assert_eq!(normalize_collision_energy("[]"), None);
        assert_eq!(normalize_collision_energy(""), None);
    }

    #[test]
    fn polarity() {
        for (ionmode, charge) in [
            (Some("negative"), Some("-1")),
            (Some("NEGATIVE"), Some("-2")),
            (Some("Positive"), Some("1")),
            (None, Some("-1")),
        ] {
            assert!(
                check_polarity(ionmode, charge).is_ok(),
                "{ionmode:?} {charge:?}"
            );
        }
        for (ionmode, charge) in [
            (Some("negative"), Some("1")),
            (Some("Positive"), Some("-1")),
            (Some("positive"), Some("0")),
            (Some("negative"), None),
        ] {
            assert!(
                check_polarity(ionmode, charge).is_err(),
                "{ionmode:?} {charge:?}"
            );
        }
    }

    #[test]
    fn end_to_end() -> anyhow::Result<()> {
        let crlf = RECORD.replace('\n', "\r\n");
        let bad_formula = RECORD.replace("FORMULA=C18H22N2O4S", "FORMULA=CH4");
        let single_energy = RECORD
            .replace(
                "COLLISION_ENERGY=[20.0, 15.0, 60.0]",
                "COLLISION_ENERGY=60.0",
            )
            .replace("SPECTYPE=ALL_ENERGIES", "SPECTYPE=SAME_ENERGY")
            .replace("RTINSECONDS=48.22", "RTINSECONDS=48")
            .replace("IONMODE=Negative", "IONMODE=NEGATIVE");
        let dir = tempfile::tempdir()?;
        let input = dir.path().join("sample_ms2.mgf");
        std::fs::write(&input, [crlf, bad_formula, single_energy].join("\n"))?;
        let output = dir.path().join("out/dataset.parquet");

        let stats = build_dataset(std::slice::from_ref(&input), &output)?;
        assert_eq!(
            stats,
            [FileStats {
                file: "sample_ms2.mgf".to_string(),
                accepted: 2,
                skipped: 1,
            }]
        );

        let batches = ParquetRecordBatchReaderBuilder::try_new(File::open(&output)?)?
            .build()?
            .collect::<Result<Vec<_>, _>>()?;
        let [batch] = batches.as_slice() else {
            panic!("expected one batch, got {}", batches.len());
        };
        assert_eq!(batch.num_rows(), 2);

        let schema = batch.schema();
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names[..11],
            [
                "identifier",
                "mzs",
                "intensities",
                "smiles",
                "inchikey",
                "formula",
                "parent_mass",
                "precursor_mz",
                "adduct",
                "instrument_type",
                "collision_energy",
            ]
        );
        assert_eq!(names.last(), Some(&"source_file"));
        assert!(!names.contains(&"num_peaks"));

        let column = |name: &str| {
            batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("missing column {name}"))
                .as_string::<i32>()
                .clone()
        };
        let row_values = |name: &str| -> Vec<String> {
            column(name)
                .iter()
                .map(|v| v.unwrap().to_string())
                .collect()
        };
        assert_eq!(
            row_values("identifier"),
            ["MSnLibID0000001", "MSnLibID0000002"]
        );
        for row in row_values("smiles") {
            assert_eq!(row, "CSCC[C@H](NC(=O)CCn1cc(C(C)=O)c2ccccc21)C(=O)O");
        }
        for row in row_values("formula") {
            assert_eq!(row, "C18H22N2O4S");
        }
        for row in row_values("source_instrument") {
            assert_eq!(row, "Orbitrap ID-X");
        }
        // `Negative` and `NEGATIVE` collapse to one spelling.
        assert_eq!(row_values("ionmode"), ["negative", "negative"]);
        for row in row_values("inchikey") {
            assert_eq!(row, "OTVRPAYVXOGAPS-HNNXBMFYSA-N");
        }
        // Mixed list/scalar values keep collision_energy a string column.
        assert_eq!(row_values("collision_energy"), ["20.0,15.0,60.0", "60.0"]);
        assert_eq!(row_values("scans"), ["174-176", "174-176"]);

        let column_of = |name: &str, data_type: DataType| {
            let array = batch
                .column_by_name(name)
                .unwrap_or_else(|| panic!("missing column {name}"));
            assert_eq!(array.data_type(), &data_type, "{name}");
            array.clone()
        };
        let floats = |name: &str| -> Vec<f64> {
            column_of(name, DataType::Float64)
                .as_primitive::<arrow_array::types::Float64Type>()
                .iter()
                .map(|v| v.unwrap())
                .collect()
        };
        let ints = |name: &str| -> Vec<i64> {
            column_of(name, DataType::Int64)
                .as_primitive::<arrow_array::types::Int64Type>()
                .iter()
                .map(|v| v.unwrap())
                .collect()
        };
        assert_eq!(floats("parent_mass"), [362.13003, 362.13003]);
        assert_eq!(floats("precursor_mz"), [361.12275, 361.12275]);
        // `48.22` and `48`: one float value widens the whole column to Float64.
        assert_eq!(floats("rtinseconds"), [48.22, 48.0]);
        assert_eq!(floats("feature_ms1_height"), [4.155e6, 4.155e6]);
        // `1.0` is written as a float, so the column stays Float64.
        assert_eq!(floats("precursor_purity"), [1.0, 1.0]);
        assert_eq!(ints("charge"), [-1, -1]);
        assert_eq!(ints("mslevel"), [2, 2]);

        for (mzs, intensities) in row_values("mzs").iter().zip(row_values("intensities")) {
            // Peaks 1, 7 and 23 are 0.257, 0.17 and 4.379 over a base peak of 100;
            // plain division prints them as 0.0025700000000000002, 0.0017000000000000001
            // and 0.043789999999999996.
            let text: Vec<&str> = intensities.split(',').collect();
            assert_eq!(
                [text[0], text[6], text[22]],
                ["0.00257", "0.0017", "0.04379"]
            );
            let intensities: Vec<f64> = text.iter().map(|v| v.parse()).collect::<Result<_, _>>()?;
            assert_eq!(intensities.len(), 36);
            assert_eq!(mzs.split(',').count(), 36);
            assert_eq!(intensities.iter().filter(|&&v| v == 1.0).count(), 1);
            assert!(intensities.iter().all(|&v| v <= 1.0));
        }
        Ok(())
    }
}
