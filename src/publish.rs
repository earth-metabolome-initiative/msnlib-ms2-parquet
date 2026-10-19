use std::path::Path;

use anyhow::{Context, anyhow, bail};
use zenodo_rs::{
    AccessRight, Creator, DepositMetadataUpdate, DepositionId, RelatedIdentifier, UploadSpec,
    UploadType, ZenodoClient,
};

use crate::MSNLIB_V8_DOI;

const TITLE: &str = "MSnLib MS2 spectra in MassSpecGym format (derived from MSnLib v8)";
const LICENSE: &str = "cc-by-4.0";
const KEYWORDS: [&str; 6] = [
    "mass spectrometry",
    "MS/MS",
    "MSnLib",
    "MassSpecGym",
    "spectral library",
    "metabolomics",
];
const DESCRIPTION_HTML: &str = "\
<p>MS2 spectra of MSnLib v8 converted to a single Parquet table in the MassSpecGym layout, one row per spectrum.</p>\
<p>Source: all 18 <code>*_ms2.mgf</code> files of MSnLib v8 (doi:10.5281/zenodo.21105617; Brungs, Schmid, Pluskal; \
Nat Methods doi:10.1038/s41592-025-02813-0), licensed CC-BY-4.0.</p>\
<p>Processing: the files were parsed with mascot-rs in strict mode; records failing SMILES, formula, \
adduct&ndash;charge&ndash;ion-mode, retention-time or peak validation were excluded.</p>\
<p>Normalization: intensities are divided by the spectrum's highest peak (max = 1.0). \
<code>collision_energy</code> lists are flattened to comma-separated values.</p>\
<p>Columns follow MassSpecGym: identifier, mzs, intensities, smiles, inchikey (full InChIKey from INCHIAUX), formula, \
parent_mass (EXACTMASS), precursor_mz (PEPMASS), adduct, instrument_type, collision_energy; followed by all remaining \
MSnLib MGF fields in lowercase and source_file.</p>\
<p>MGF fields whose values are all integers (e.g. charge, mslevel) are 64-bit integers, those whose values are all \
numeric (e.g. parent_mass, precursor_mz, rtinseconds) are 64-bit floats; all other columns are strings, and mzs and \
intensities are comma-separated strings.</p>";

/// Publishes `file` as a Zenodo dataset derived from MSnLib v8: a new record,
/// or a new version of deposition `new_version` whose files are replaced.
pub async fn run(
    file: &Path,
    sandbox: bool,
    dry_run: bool,
    new_version: Option<u64>,
    dataset_version: &str,
) -> anyhow::Result<()> {
    let metadata = deposit_metadata(creator_from_env()?, dataset_version)?;
    let len = std::fs::metadata(file)
        .map_err(|_| anyhow!("{} not found; run `msnlib-rs build` first", file.display()))?
        .len();
    let file_name = file
        .file_name()
        .and_then(|name| name.to_str())
        .with_context(|| format!("{} has no UTF-8 file name", file.display()))?;
    let host = if sandbox { "sandbox zenodo" } else { "zenodo" };
    let target = match new_version {
        Some(id) => format!("{host} as {dataset_version}, a new version of deposition {id}"),
        None => format!("{host} as {dataset_version}, a new record"),
    };

    if dry_run {
        println!("{}", serde_json::to_string_pretty(&metadata)?);
        eprintln!("would upload {file_name} ({len} B) to {target}");
        return Ok(());
    }

    let client = if sandbox {
        ZenodoClient::from_sandbox_env()?
    } else {
        ZenodoClient::from_env()?
    };
    let files = UploadSpec::from_named_paths([(file_name, file)])?;
    eprintln!("uploading {file_name} ({len} B) to {target}");
    let published = match new_version {
        Some(id) => {
            client
                .publish_dataset(DepositionId(id), &metadata, files)
                .await?
        }
        None => client.create_and_publish_dataset(&metadata, files).await?,
    };
    let doi =
        |doi: Option<zenodo_rs::Doi>| doi.map_or_else(|| "-".to_string(), |doi| doi.to_string());
    eprintln!("published record {}", published.record.id);
    eprintln!("doi: {}", doi(published.record.doi));
    eprintln!("concept doi: {}", doi(published.record.conceptdoi));
    eprintln!(
        "next version: msnlib-rs publish --new-version {} --dataset-version <LABEL>",
        published.deposition.id
    );
    Ok(())
}

fn creator_from_env() -> anyhow::Result<Creator> {
    let var = |name: &str| std::env::var(name).ok().filter(|value| !value.is_empty());
    let Some(name) = var("ZENODO_CREATOR") else {
        bail!("ZENODO_CREATOR must be set, e.g. 'Family, Given' (see .env.example)");
    };
    let mut builder = Creator::builder().name(name);
    if let Some(orcid) = var("ZENODO_ORCID") {
        builder = builder.orcid(orcid);
    }
    if let Some(affiliation) = var("ZENODO_AFFILIATION") {
        builder = builder.affiliation(affiliation);
    }
    Ok(builder.build()?)
}

fn deposit_metadata(creator: Creator, version: &str) -> anyhow::Result<DepositMetadataUpdate> {
    let mut builder = DepositMetadataUpdate::builder()
        .title(TITLE)
        .upload_type(UploadType::Dataset)
        .description_html(DESCRIPTION_HTML)
        .creator(creator)
        .access_right(AccessRight::Open)
        .license(LICENSE)
        .version(version)
        .related_identifier(
            RelatedIdentifier::builder()
                .identifier(MSNLIB_V8_DOI)
                .relation("isDerivedFrom")
                .scheme("doi")
                .resource_type("dataset")
                .build()?,
        );
    for keyword in KEYWORDS {
        builder = builder.keyword(keyword);
    }
    Ok(builder.build()?)
}
