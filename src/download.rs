use std::path::Path;

use anyhow::bail;
use zenodo_rs::{RecordFile, RecordId, ZenodoClient};

use crate::{MS2_SUFFIX, MSNLIB_V8_RECORD_ID};

/// Downloads every `*ms2.mgf` file of MSnLib v8 into `dir`.
///
/// Files already present with the size reported by Zenodo are skipped unless
/// `force` is set; zenodo-rs only persists a download after its md5 checksum
/// matched, so an existing file of the right size was verified when written.
pub async fn run(dir: &Path, force: bool) -> anyhow::Result<()> {
    let client = ZenodoClient::anonymous()?;
    let record = client.get_record(RecordId(MSNLIB_V8_RECORD_ID)).await?;

    let mut files: Vec<&RecordFile> = record
        .files
        .iter()
        .filter(|file| file.key.ends_with(MS2_SUFFIX))
        .collect();
    files.sort_by(|a, b| a.key.cmp(&b.key));
    if files.is_empty() {
        bail!("record {} has no *{MS2_SUFFIX} files", record.id);
    }

    std::fs::create_dir_all(dir)?;
    for file in files {
        let target = dir.join(&file.key);
        if !force && std::fs::metadata(&target).is_ok_and(|metadata| metadata.len() == file.size) {
            eprintln!("skip {} (already present)", file.key);
            continue;
        }
        let download = client
            .download_record_file_by_key_to_path(record.id, &file.key, &target)
            .await?;
        eprintln!("downloaded {} ({} B)", file.key, download.bytes_written);
    }
    Ok(())
}
