#!/usr/bin/env python3

"""Hash the staged, load-bearing NORA source without approval metadata."""

from __future__ import annotations

import hashlib
import os
import subprocess
import sys


EXCLUDED_FILE = b"scripts/redb-production-allowlist.txt"
EXCLUDED_PREFIX = b"scripts/redb-production-evidence/"
DOMAIN = b"nora-load-bearing-source-v1\0"


def git(root: str, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", root, *args])


def main() -> int:
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <git-worktree>", file=sys.stderr)
        return 2

    root = os.path.realpath(sys.argv[1])
    records = git(root, "ls-files", "--stage", "-z").split(b"\0")
    digest = hashlib.sha256(DOMAIN)
    for record in records:
        if not record:
            continue
        metadata, path = record.split(b"\t", 1)
        if path == EXCLUDED_FILE or path.startswith(EXCLUDED_PREFIX):
            continue
        mode, object_id, stage = metadata.split(b" ", 2)
        if stage != b"0":
            raise SystemExit("release blocked: source index contains an unmerged entry")
        oid = object_id.decode("ascii")
        object_type = git(root, "cat-file", "-t", oid).strip()
        contents = git(root, "cat-file", object_type.decode("ascii"), oid)
        for field in (mode, object_type, path, contents):
            digest.update(len(field).to_bytes(8, "big"))
            digest.update(field)

    print(digest.hexdigest())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
