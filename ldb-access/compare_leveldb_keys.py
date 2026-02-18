#!/usr/bin/env python3
"""Compare keys and ids between two LevelDB directories."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict, Iterator, Set, Tuple


class LevelDBUnavailableError(RuntimeError):
    pass


def _load_leveldb_reader():
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


def _key_to_text(key: bytes) -> str:
    try:
        return key.decode("utf-8")
    except UnicodeDecodeError:
        return repr(key)


def _extract_id(key_text: str) -> str:
    if key_text.startswith("!"):
        parts = key_text.split("!")
        if len(parts) >= 3:
            return parts[2]
    return ""


def _load_keys(db_dir: Path) -> Set[str]:
    reader = _load_leveldb_reader()
    return {_key_to_text(k) for k, _ in reader(db_dir)}


def _summarize(keys: Set[str]) -> Dict[str, int]:
    ids = {_extract_id(k) for k in keys if _extract_id(k)}
    return {"keys": len(keys), "ids": len(ids)}


def main() -> int:
    p = argparse.ArgumentParser(description="Compare two LevelDB key sets")
    p.add_argument("left", help="Left DB directory")
    p.add_argument("right", help="Right DB directory")
    args = p.parse_args()

    left = Path(args.left).resolve()
    right = Path(args.right).resolve()

    left_keys = _load_keys(left)
    right_keys = _load_keys(right)

    only_left = sorted(left_keys - right_keys)
    only_right = sorted(right_keys - left_keys)

    print(f"left:  {left} -> {_summarize(left_keys)}")
    print(f"right: {right} -> {_summarize(right_keys)}")
    print(f"only_left: {len(only_left)}")
    print(f"only_right: {len(only_right)}")

    if only_left:
        print("sample only_left:")
        for k in only_left[:20]:
            print(f"  {k}")
    if only_right:
        print("sample only_right:")
        for k in only_right[:20]:
            print(f"  {k}")

    return 0 if not only_left and not only_right else 2


if __name__ == "__main__":
    raise SystemExit(main())
