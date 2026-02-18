#!/usr/bin/env python3
"""Rebuild LevelDB databases from extracted JSON structure.

Reads out_root/<db>/manifest.json for known records and also ingests new record
files added manually under the DB output tree.

Writes rebuilt LevelDB databases to deploy_root/<db>/.
"""

from __future__ import annotations

import argparse
import base64
import json
import random
import string
import shutil
import time
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


class LevelDBUnavailableError(RuntimeError):
    pass


def _load_leveldb_iterator():
    try:
        import plyvel  # type: ignore

        def iterator(db_path: Path) -> Iterable[Tuple[bytes, bytes]]:
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

        def iterator(db_path: Path) -> Iterable[Tuple[bytes, bytes]]:
            db = leveldb.LevelDB(str(db_path), create_if_missing=False)
            yield from db.RangeIter()

        return iterator
    except Exception as exc:
        raise LevelDBUnavailableError(
            "No Python LevelDB backend found. Install one of: 'plyvel' or 'leveldb'."
        ) from exc


class LevelDBWriter:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._backend = None
        self._open()

    def _open(self) -> None:
        try:
            import plyvel  # type: ignore

            self._backend = ("plyvel", plyvel.DB(str(self.db_path), create_if_missing=True))
            return
        except Exception:
            pass

        try:
            import leveldb  # type: ignore

            self._backend = ("leveldb", leveldb.LevelDB(str(self.db_path), create_if_missing=True))
            return
        except Exception as exc:
            raise LevelDBUnavailableError(
                "No Python LevelDB backend found. Install one of: 'plyvel' or 'leveldb'."
            ) from exc

    def put(self, key: bytes, value: bytes) -> None:
        backend_name, backend = self._backend
        if backend_name == "plyvel":
            backend.put(key, value)
        else:
            backend.Put(key, value)

    def close(self) -> None:
        if self._backend and self._backend[0] == "plyvel":
            self._backend[1].close()


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _safe_name(name: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in str(name).strip())
    return cleaned.strip("_") or "unnamed"


def _record_title(value, fallback: str) -> str:
    if isinstance(value, dict):
        for field in ("name", "title", "label", "_id"):
            current = value.get(field)
            if isinstance(current, str) and current.strip():
                return current.strip()
    return fallback


def _decode_value(raw: bytes):
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return {"base64": base64.b64encode(raw).decode("ascii")}
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def _looks_like_base64_wrapper(value) -> bool:
    return isinstance(value, dict) and set(value.keys()) == {"base64"} and isinstance(value["base64"], str)


def _encode_value(value) -> bytes:
    if _looks_like_base64_wrapper(value):
        return base64.b64decode(value["base64"])
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def _iter_db_dirs(out_root: Path) -> Iterable[Path]:
    if not out_root.exists():
        return []
    return sorted([p for p in out_root.iterdir() if p.is_dir()])


def _excluded_json_names() -> Set[str]:
    return {
        "full.json",
        "folders.json",
        "manifest.json",
    }


def _infer_root_from_path(rel_path: Path) -> Optional[str]:
    parts = rel_path.parts
    if not parts:
        return None

    if parts[0] == "_tree":
        if len(parts) < 3:
            return None
        return parts[-2]

    if parts[0].startswith("_"):
        return None

    return parts[0]


def _infer_id(value, rel_path: Path) -> str:
    if isinstance(value, dict):
        vid = value.get("_id")
        if isinstance(vid, str) and vid.strip():
            return vid.strip()

    stem = rel_path.stem
    if "__" in stem:
        guessed = stem.rsplit("__", 1)[-1]
        if guessed:
            return guessed

    return _safe_name(stem)


def _filename_has_id(rel_path: Path) -> bool:
    stem = rel_path.stem
    if "__" not in stem:
        return False
    return bool(stem.rsplit("__", 1)[-1])


def _extract_id_from_key(key: str) -> Optional[str]:
    if not key.startswith("!"):
        return None
    parts = key.split("!")
    if len(parts) < 3:
        return None
    rec_id = parts[2].strip()
    return rec_id or None


def _random_id(used_ids: Set[str]) -> str:
    alphabet = string.ascii_letters + string.digits
    while True:
        candidate = "".join(random.choice(alphabet) for _ in range(16))
        if candidate not in used_ids:
            return candidate


def _replace_compendium_source_id(stats: dict, new_id: str) -> bool:
    source = stats.get("compendiumSource")
    if not isinstance(source, str) or not source:
        return False
    if "." in source:
        stats["compendiumSource"] = f"{source.rsplit('.', 1)[0]}.{new_id}"
    else:
        stats["compendiumSource"] = new_id
    return True


def _record_files_for_new_entries(db_out_dir: Path, known_paths: Set[str]) -> List[Path]:
    files: List[Path] = []
    for path in sorted(db_out_dir.rglob("*.json")):
        rel = path.relative_to(db_out_dir)
        rel_str = str(rel)
        # Top-level JSON files are metadata/aggregates, not single-record inputs.
        if rel.parent == Path("."):
            continue
        if rel.name in _excluded_json_names():
            continue
        if rel_str in known_paths:
            continue
        root = _infer_root_from_path(rel)
        if not root:
            continue
        files.append(path)
    return files


def _load_manifest(db_out_dir: Path) -> Tuple[List[dict], dict]:
    manifest_path = db_out_dir / "manifest.json"
    if not manifest_path.exists():
        return [], {}
    data = _read_json(manifest_path)
    if isinstance(data, list):
        # Backward compatibility with older extractor output.
        return [x for x in data if isinstance(x, dict)], {}
    if isinstance(data, dict):
        records = data.get("records")
        if not isinstance(records, list):
            records = []
        leveldb_meta = data.get("leveldb_meta")
        if not isinstance(leveldb_meta, dict):
            leveldb_meta = {}
        return [x for x in records if isinstance(x, dict)], leveldb_meta
    return [], {}


def _build_known_records(db_out_dir: Path, manifest: List[dict]) -> List[Tuple[str, bytes]]:
    records: List[Tuple[str, bytes]] = []
    for item in manifest:
        key = item.get("key")
        rel_path = item.get("path")
        if not isinstance(key, str) or not isinstance(rel_path, str):
            continue

        value_path = db_out_dir / rel_path
        if not value_path.exists():
            print(f"Skipping missing record file: {value_path}")
            continue

        value = _read_json(value_path)
        records.append((key, _encode_value(value)))
    return records


def _build_new_records(db_out_dir: Path, known_paths: Set[str], used_ids: Set[str]) -> List[Tuple[str, bytes]]:
    records: List[Tuple[str, bytes]] = []
    for path in _record_files_for_new_entries(db_out_dir, known_paths):
        rel = path.relative_to(db_out_dir)
        value = _read_json(path)
        root = _infer_root_from_path(rel)
        if not root:
            continue
        generated_id: Optional[str] = None
        changed = False
        if not _filename_has_id(rel):
            generated_id = _random_id(used_ids)
            used_ids.add(generated_id)
            new_path = path.with_name(f"{rel.stem}__{generated_id}{path.suffix}")
            path.rename(new_path)
            path = new_path
            rel = path.relative_to(db_out_dir)
            print(f"{_record_title(value, rel.stem)}: appended generated id to filename")
            changed = True

        if isinstance(value, dict):
            if generated_id:
                value["_id"] = generated_id
                stats = value.get("_stats")
                if isinstance(stats, dict):
                    _replace_compendium_source_id(stats, generated_id)
                changed = True

            current_id = value.get("_id")
            if not isinstance(current_id, str) or not current_id.strip():
                created_id = _random_id(used_ids)
                value["_id"] = created_id
                stats = value.get("_stats")
                if isinstance(stats, dict):
                    _replace_compendium_source_id(stats, created_id)
                print(f"{_record_title(value, rel.stem)}: created id for new element")
                used_ids.add(created_id)
                changed = True

        if changed:
            _write_json(path, value)
        rec_id = _infer_id(value, rel)
        used_ids.add(rec_id)
        key = f"!{root}!{rec_id}"
        records.append((key, _encode_value(value)))
    return records


def _snapshot_db(iterator_factory, db_dir: Path) -> Dict[str, bytes]:
    rows: Dict[str, bytes] = {}
    if not db_dir.exists():
        return rows
    for key, value in iterator_factory(db_dir):
        try:
            k = key.decode("utf-8")
        except UnicodeDecodeError:
            k = base64.b64encode(key).decode("ascii")
        rows[k] = value
    return rows


def _values_differ(left: bytes, right: bytes) -> bool:
    if left == right:
        return False
    return _decode_value(left) != _decode_value(right)


def _print_db_diff(db_name: str, source_rows: Dict[str, bytes], rebuilt_rows: Dict[str, bytes]) -> None:
    source_keys = set(source_rows.keys())
    rebuilt_keys = set(rebuilt_rows.keys())
    added = rebuilt_keys - source_keys
    removed = source_keys - rebuilt_keys
    changed = {k for k in (source_keys & rebuilt_keys) if _values_differ(source_rows[k], rebuilt_rows[k])}

    if not added and not removed and not changed:
        print(f"{db_name}: no element differences between source and rebuilt idb")
        return

    print(
        f"{db_name}: element differences found "
        f"(added={len(added)}, removed={len(removed)}, changed={len(changed)})"
    )

    names: List[str] = []
    for key in sorted(added):
        names.append(_record_title(_decode_value(rebuilt_rows[key]), key))
    for key in sorted(removed):
        names.append(_record_title(_decode_value(source_rows[key]), key))
    for key in sorted(changed):
        names.append(_record_title(_decode_value(rebuilt_rows[key]), key))

    for name in sorted(set(names)):
        print(name)


def rebuild_all(out_root: Path, deploy_root: Path, source_root: Path) -> int:
    started = time.perf_counter()
    db_dirs = list(_iter_db_dirs(out_root))
    if not db_dirs:
        print(f"No DB directories found under {out_root}")
        return 1

    iterator_factory = _load_leveldb_iterator()
    deploy_root.mkdir(parents=True, exist_ok=True)
    folders_processed = 0
    files_processed = 0

    for db_out_dir in db_dirs:
        db_name = db_out_dir.name
        deploy_db_dir = deploy_root / db_name
        source_db_dir = source_root / db_name

        if deploy_db_dir.exists():
            shutil.rmtree(deploy_db_dir)

        print(f"Rebuilding {db_name} -> {deploy_db_dir}")
        writer = LevelDBWriter(deploy_db_dir)
        try:
            manifest, leveldb_meta = _load_manifest(db_out_dir)
            known_paths = {str(item.get("path")) for item in manifest if isinstance(item.get("path"), str)}
            used_ids = {
                rec_id
                for rec_id in (
                    _extract_id_from_key(str(item.get("key")))
                    for item in manifest
                    if isinstance(item.get("key"), str)
                )
                if rec_id
            }

            known_records = _build_known_records(db_out_dir, manifest)
            new_records = _build_new_records(db_out_dir, known_paths, used_ids)
            files_processed += len(known_records) + len(new_records)

            merged: Dict[str, bytes] = {}
            for key, value in known_records:
                merged[key] = value
            for key, value in new_records:
                merged[key] = value

            for key, value in merged.items():
                writer.put(key.encode("utf-8"), value)

            comparator = leveldb_meta.get("comparator")
            if isinstance(comparator, str) and comparator and comparator != "leveldb.BytewiseComparator":
                print(
                    f"Warning: source comparator is {comparator}; rebuild uses default "
                    "LevelDB comparator (leveldb.BytewiseComparator)."
                )

            print(
                f"Wrote {len(known_records)} manifest records, "
                f"{len(new_records)} new records ({len(merged)} total keys)"
            )
        finally:
            writer.close()

        source_rows = _snapshot_db(iterator_factory, source_db_dir)
        rebuilt_rows = _snapshot_db(iterator_factory, deploy_db_dir)
        _print_db_diff(db_name, source_rows, rebuilt_rows)
        folders_processed += 1

    elapsed = time.perf_counter() - started
    print("Rebuild stats:")
    print(f"duration_seconds: {elapsed:.3f}")
    print(f"folders_processed: {folders_processed}")
    print(f"files_processed: {files_processed}")

    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rebuild LevelDB databases from extracted JSON")
    parser.add_argument(
        "out_root",
        nargs="?",
        default="out",
        help="Root directory containing extracted DB folders (default: out)",
    )
    parser.add_argument(
        "deploy_root",
        nargs="?",
        default="deploy",
        help="Output root directory for rebuilt LevelDB databases (default: deploy)",
    )
    parser.add_argument(
        "source_root",
        nargs="?",
        default="in",
        help="Source LevelDB root for diff comparison (default: in)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    out_root = Path(args.out_root).resolve()
    deploy_root = Path(args.deploy_root).resolve()
    source_root = Path(args.source_root).resolve()
    return rebuild_all(out_root, deploy_root, source_root)


if __name__ == "__main__":
    raise SystemExit(main())
