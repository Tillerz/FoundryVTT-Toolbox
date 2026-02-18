#!/usr/bin/env python3
"""Extract LevelDB data into editable JSON files with round-trip metadata.

Output for each discovered DB under in_root:
- out_root/<db>/full.json            (all extracted records)
- out_root/<db>/manifest.json        (DB metadata + key -> record file mapping)
- out_root/<db>/folders.json         (folder ids + computed paths)
- out_root/<db>/<root>/*.json        (records without resolved folder path)
- out_root/<db>/_tree/<path>/<root>/*.json (records placed by folder tree)

Record files store only the record value so they are easy to edit.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import time
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Optional, Tuple


class LevelDBUnavailableError(RuntimeError):
    pass


def _load_leveldb_backend():
    try:
        import plyvel  # type: ignore

        def iterator(db_path: Path) -> Iterator[Tuple[bytes, bytes]]:
            db = plyvel.DB(str(db_path), create_if_missing=False)
            try:
                yield from db
            finally:
                db.close()

        return iterator
    except Exception:
        pass

    try:
        import leveldb  # type: ignore

        def iterator(db_path: Path) -> Iterator[Tuple[bytes, bytes]]:
            db = leveldb.LevelDB(str(db_path), create_if_missing=False)
            for key, value in db.RangeIter():
                yield key, value

        return iterator
    except Exception as exc:
        raise LevelDBUnavailableError(
            "No Python LevelDB backend found. Install one of: 'plyvel' or 'leveldb'."
        ) from exc


def _encode_bytes(raw: bytes):
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        return {"base64": base64.b64encode(raw).decode("ascii")}


def _coerce_value(raw: bytes, parse_json_values: bool):
    encoded = _encode_bytes(raw)
    if not parse_json_values or not isinstance(encoded, str):
        return encoded

    try:
        return json.loads(encoded)
    except json.JSONDecodeError:
        return encoded


def _parse_key_parts(raw: bytes) -> Tuple[str, str, str]:
    """Extract root entry + id + full key from keys like b'!journal!abc123'."""
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return "binary", "", ""

    if text.startswith("!"):
        parts = text.split("!")
        if len(parts) >= 3 and parts[1]:
            return parts[1], parts[2], text
    return "misc", text, text


def _safe_name(name: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(name).strip())
    return cleaned.strip("_") or "unnamed"


def _record_title(value, fallback: str) -> str:
    if isinstance(value, dict):
        for field in ("name", "title", "label", "_id"):
            current = value.get(field)
            if isinstance(current, str) and current.strip():
                return _safe_name(current)
    return _safe_name(fallback)


def _folder_payload(record: dict) -> Optional[dict]:
    value = record.get("value")
    if isinstance(value, dict):
        return value
    return None


def _build_folder_index(folder_records: List[dict]) -> Dict[str, dict]:
    folder_by_id: Dict[str, dict] = {}
    for record in folder_records:
        payload = _folder_payload(record)
        if not payload:
            continue
        folder_id = str(payload.get("_id") or record.get("id") or "").strip()
        if folder_id:
            folder_by_id[folder_id] = payload
    return folder_by_id


def _compute_folder_path(folder_id: str, folder_by_id: Dict[str, dict], memo: Dict[str, Path]) -> Optional[Path]:
    if folder_id in memo:
        return memo[folder_id]

    folder = folder_by_id.get(folder_id)
    if not folder:
        return None

    name = _safe_name(str(folder.get("name", folder_id)))
    parent_id = folder.get("folder")
    if isinstance(parent_id, str) and parent_id and parent_id in folder_by_id and parent_id != folder_id:
        parent = _compute_folder_path(parent_id, folder_by_id, memo)
        if parent is None:
            memo[folder_id] = Path(name)
            return memo[folder_id]
        memo[folder_id] = parent / name
        return memo[folder_id]

    memo[folder_id] = Path(name)
    return memo[folder_id]


def _discover_ldb_files(root: Path, recursive: bool) -> Iterable[Path]:
    pattern = "**/*.ldb" if recursive else "*.ldb"
    return sorted(root.glob(pattern))


def _read_text_if_exists(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8", errors="replace").strip()


def _find_comparator_in_manifest(manifest_path: Path) -> Optional[str]:
    if not manifest_path.exists():
        return None
    data = manifest_path.read_bytes()
    # Default LevelDB DBs typically use leveldb.BytewiseComparator.
    match = re.search(rb"leveldb\.[A-Za-z0-9_.-]*Comparator", data)
    if not match:
        return None
    return match.group(0).decode("utf-8", errors="replace")


def _collect_leveldb_meta(db_dir: Path) -> dict:
    current_value = _read_text_if_exists(db_dir / "CURRENT")
    manifest_name = current_value if current_value and current_value.startswith("MANIFEST-") else None
    manifest_path = db_dir / manifest_name if manifest_name else None
    comparator = _find_comparator_in_manifest(manifest_path) if manifest_path else None

    return {
        "source_dir": db_dir.name,
        "current": current_value,
        "manifest_file": manifest_name,
        "comparator": comparator,
        "source_files": sorted(
            p.name
            for p in db_dir.iterdir()
            if p.is_file() and (p.suffix in {".ldb", ".log"} or p.name.startswith("MANIFEST-") or p.name in {"CURRENT", "LOG", "LOG.old", "LOCK"})
        ),
    }


def _extract_records(iterator_factory, db_dir: Path, parse_json_values: bool) -> List[dict]:
    records: List[dict] = []
    for key, value in iterator_factory(db_dir):
        root_entry, doc_id, key_text = _parse_key_parts(key)
        records.append(
            {
                "root": _safe_name(root_entry),
                "id": doc_id,
                "key": key_text or _encode_bytes(key),
                "value": _coerce_value(value, parse_json_values=parse_json_values),
            }
        )
    return records


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _place_records(out_dir: Path, records: List[dict], leveldb_meta: dict) -> None:
    grouped: Dict[str, List[dict]] = {}
    for rec in records:
        grouped.setdefault(rec["root"], []).append(rec)

    folder_records = grouped.get("folders", [])
    folder_by_id = _build_folder_index(folder_records)
    folder_paths: Dict[str, Path] = {}

    folder_manifest: List[dict] = []
    for folder_id, payload in sorted(folder_by_id.items()):
        rel_path = _compute_folder_path(folder_id, folder_by_id, folder_paths)
        if rel_path is None:
            continue
        folder_manifest.append(
            {
                "id": folder_id,
                "name": payload.get("name"),
                "parent_id": payload.get("folder"),
                "path": str(rel_path),
                "type": payload.get("type"),
            }
        )

    _write_json(out_dir / "folders.json", folder_manifest)

    manifest: List[dict] = []
    for rec in records:
        root = rec["root"]
        rec_id = str(rec.get("id") or "").strip() or "unknown"
        value = rec.get("value")

        title = _record_title(value, fallback=rec_id)
        file_name = f"{title}__{_safe_name(rec_id)}.json"

        rel_dir: Optional[Path] = None
        if isinstance(value, dict):
            folder_id = value.get("folder")
            if isinstance(folder_id, str) and folder_id:
                folder_rel = _compute_folder_path(folder_id, folder_by_id, folder_paths)
                if folder_rel is not None:
                    rel_dir = Path("_tree") / folder_rel / root

        if rel_dir is None:
            rel_dir = Path(root)

        rel_path = rel_dir / file_name
        _write_json(out_dir / rel_path, value)

        manifest.append(
            {
                "key": rec["key"],
                "root": root,
                "id": rec_id,
                "path": str(rel_path),
            }
        )

    _write_json(
        out_dir / "manifest.json",
        {
            "schema_version": 2,
            "leveldb_meta": leveldb_meta,
            "records": manifest,
        },
    )

    # Keep grouped files for reference/debugging.
    for root, rows in sorted(grouped.items()):
        _write_json(out_dir / f"{root}.json", rows)


def dump_all(in_root: Path, out_root: Path, recursive: bool, parse_json_values: bool) -> int:
    started = time.perf_counter()
    iterator_factory = _load_leveldb_backend()
    ldb_files = list(_discover_ldb_files(in_root, recursive=recursive))

    if not ldb_files:
        print(f"No .ldb files found under {in_root}")
        return 1

    processed_db_dirs: set[Path] = set()

    for ldb_file in ldb_files:
        db_dir = ldb_file.parent
        rel_dir = db_dir.relative_to(in_root)
        out_dir = out_root / rel_dir

        if db_dir in processed_db_dirs:
            print(f"Skipping {ldb_file}: database directory already dumped")
            continue

        print(f"Reading {db_dir}")
        records = _extract_records(iterator_factory, db_dir, parse_json_values=parse_json_values)
        leveldb_meta = _collect_leveldb_meta(db_dir)
        _write_json(out_dir / "full.json", records)
        _place_records(out_dir, records, leveldb_meta=leveldb_meta)
        print(f"Finished {db_dir}: {len(records)} total entries")
        processed_db_dirs.add(db_dir)

    elapsed = time.perf_counter() - started
    print("Conversion stats:")
    print(f"duration_seconds: {elapsed:.3f}")
    print(f"folders_processed: {len(processed_db_dirs)}")
    print(f"files_processed: {len(ldb_files)}")

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract LevelDB data and organize records by folder references"
    )
    parser.add_argument(
        "in_root",
        nargs="?",
        default="in",
        help="Input root directory to scan for LevelDB folders (default: in)",
    )
    parser.add_argument(
        "out_root",
        nargs="?",
        default="out",
        help="Output root directory for JSON files (default: out)",
    )
    parser.add_argument(
        "--no-recursive",
        action="store_true",
        help="Only scan the root directory, not subdirectories",
    )
    parser.add_argument(
        "--no-parse-json-values",
        action="store_true",
        help="Keep values as strings/bytes instead of parsing JSON strings",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    in_root = Path(args.in_root).resolve()
    out_root = Path(args.out_root).resolve()
    return dump_all(
        in_root,
        out_root,
        recursive=not args.no_recursive,
        parse_json_values=not args.no_parse_json_values,
    )


if __name__ == "__main__":
    raise SystemExit(main())
