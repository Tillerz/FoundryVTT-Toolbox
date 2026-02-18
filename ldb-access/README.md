# LevelDB JSON Tools for FoundryVTT

These scripts help FoundryVTT users work with LevelDB-backed data in bulk:

- `dump_leveldb_to_json.py`: Extracts `.ldb` databases to editable JSON files.
- `rebuild_leveldb_from_json.py`: Rebuilds LevelDB databases from those JSON files (including newly added records).
- `compare_leveldb_keys.py`: Compares keys/IDs between two LevelDB directories.

Typical FoundryVTT content in these DBs includes actors, scenes, journal entries, items, folders, and related documents.

## Requirements

- Python 3.9+
- One LevelDB backend for Python:
  - `plyvel` (preferred), or
  - `leveldb`

Install example:

```bash
pip install plyvel
```

## Folder Layout

The scripts default to this structure inside `ldb-access/`:

- `in/`: Source LevelDB DB directories from FoundryVTT
- `out/`: Extracted JSON output
- `deploy/`: Rebuilt LevelDB output

The input folder should contain module sub-folders, like `in/myown-core-system`.


You can also pass custom paths to each script.

## 1) Dump LevelDB to JSON

From `ldb-access/`:

```bash
python dump_leveldb_to_json.py
```

Default behavior:

- Scans `in/` recursively for `.ldb` files.
- Dumps each DB folder once.
- Writes structured JSON to `out/<db>/`.

Useful flags:

```bash
python dump_leveldb_to_json.py <in_root> <out_root> --no-recursive --no-parse-json-values
```

- `--no-recursive`: Only scan top-level `in_root`.
- `--no-parse-json-values`: Keep raw string/byte payload form instead of parsing JSON text.

### Dump Output

Each DB in `out/<db>/` contains:

- `full.json`: All extracted records
- `manifest.json`: Key-to-file mapping + LevelDB metadata
- `folders.json`: Computed folder tree info
- `<root>/*.json`: Record files by root type
- `_tree/<folder path>/<root>/*.json`: Records placed by Foundry folder hierarchy when possible

## 2) Edit / Add JSON Records

- Edit existing JSON files in `out/<db>/...`.
- Add new JSON files under valid root folders (for example `out/world/actors/` or under `_tree/.../<root>/`).
- New files should represent a single Foundry object.

Note: `rebuild_leveldb_from_json.py` can auto-generate IDs for new records when needed.

## 3) Rebuild LevelDB from JSON

From `ldb-access/`:

```bash
python rebuild_leveldb_from_json.py
```

Defaults:

- Reads extracted JSON from `out/`
- Rebuilds DBs into `deploy/`
- Compares rebuilt result against source DBs in `in/`

Custom paths:

```bash
python rebuild_leveldb_from_json.py <out_root> <deploy_root> <source_root>
```

Notes:

- Existing `deploy/<db>/` is replaced on rebuild.
- New JSON entries not listed in `manifest.json` are detected and added.
- If a new filename has no `__<id>` suffix, an ID is generated and appended.

## 4) Compare Two DB Directories

```bash
python compare_leveldb_keys.py <left_db_dir> <right_db_dir>
```

The script prints key/id counts, missing keys on each side, and sample differences.

## Recommended Workflow for FoundryVTT

1. Back up your Foundry world/module data.
2. Copy the relevant DB folders into `ldb-access/in/`.
3. Run `dump_leveldb_to_json.py`.
4. Batch edit/create JSON documents in `ldb-access/out/`.
5. Run `rebuild_leveldb_from_json.py`.
6. Validate diffs/log output.
7. Replace target DB folder(s) with `ldb-access/deploy/<db>/` only after verification.

## Safety Notes

- Always keep backups before replacing FoundryVTT DB files.
- Do not edit while Foundry is actively writing to the same DB files.
- Rebuild uses default LevelDB comparator (`leveldb.BytewiseComparator`); a warning is printed if source metadata indicates a different comparator.
