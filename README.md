# msnlib-rs

Builds a [MassSpecGym](https://github.com/pluskal-lab/MassSpecGym)-style Parquet table from the MS2 spectra of
[MSnLib v8](https://doi.org/10.5281/zenodo.21105617) (CC-BY-4.0), and publishes it to Zenodo as a dataset linked to
MSnLib v8 with `isDerivedFrom`.

The `*ms2.mgf` files are parsed with [mascot-rs](https://github.com/earth-metabolome-initiative/mascot-rs) in strict
mode. Records that fail its validation (SMILES, formula vs. SMILES, adduct/charge/ion mode, retention time, peaks) are
skipped and counted per file.

## Prerequisites

- Rust ≥ 1.88 (highest `rust-version` among the dependencies).
- Git access to GitHub: mascot-rs is a pinned git dependency.

## Reproduce

```sh
cp .env.example .env                      # fill in the Zenodo values before publishing
cargo run --release -- download           # 18 *ms2.mgf files (~1.19 GB) into downloads/
cargo run --release -- build              # output/msnlib_ms2.parquet
cargo run --release -- publish --dry-run  # prints the deposit metadata, uploads nothing
cargo run --release -- publish            # or: publish --sandbox
```

A later build is published as a new version of the same Zenodo concept record; the new version's files are replaced by
`--file`:

```sh
cargo run --release -- publish --new-version <DEPOSITION_ID> --dataset-version v2
```

`download` skips files already present with the size Zenodo reports (`--force` downloads again); downloads are
md5-verified before they are written. `publish` needs `ZENODO_TOKEN` (or `ZENODO_SANDBOX_TOKEN` with `--sandbox`) and
`ZENODO_CREATOR`; `ZENODO_ORCID` and `ZENODO_AFFILIATION` are optional. Without `--new-version`, every non-dry run
creates a new Zenodo record.

## Columns

Each MGF field gets the narrowest type that holds all its non-empty values in the accepted records: `Int64` when every
value is an integer (e.g. `charge`, `mslevel`), else `Float64` when every value is a finite number (e.g. `parent_mass`,
`precursor_mz`, `rtinseconds`), else string. `mzs` and `intensities` are comma-separated strings. A missing or empty
MGF value is null.

| column | source |
|---|---|
| identifier | `MSnLibID0000001`, … numbered over accepted records in file-name order, then in-file order |
| mzs | peak m/z values, comma-separated, ascending |
| intensities | peak intensities divided by the spectrum's highest peak (max = `1.0`), comma-separated |
| smiles | `SMILES` |
| inchikey | `INCHIAUX` (full InChIKey) |
| formula | `FORMULA` |
| parent_mass | `EXACTMASS` |
| precursor_mz | `PEPMASS` |
| adduct | `ADDUCT` |
| instrument_type | `INSTRUMENT_TYPE` |
| collision_energy | `COLLISION_ENERGY`, list brackets and spaces removed: `[20.0, 15.0, 60.0]` → `20.0,15.0,60.0` |
| *rest* | every other MGF header, verbatim, named in lowercase with spaces as `_`, sorted by name; `Num peaks` is dropped because it can disagree with `mzs` |
| source_file | MGF file name, e.g. `20241003_otavapep_neg_ms2.mgf` |

MassSpecGym's `fold`, `simulation_challenge` and `precursor_formula` are not emitted: MSnLib has no source data for
them.
