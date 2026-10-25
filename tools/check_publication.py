"""Inspect files intended for Git for accidental secrets and operational artifacts."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
IGNORED = {
    ".git",
    ".venv",
    "build",
    "dist",
    "__pycache__",
    ".mypy_cache",
    ".ruff_cache",
    ".pytest_cache",
    "pytest-tmp",
}
ALLOWED_CSV = "examples/synthetic_preview.csv"
BLOCKED_EXTENSIONS = {
    ".xlsx",
    ".xls",
    ".sqlite3",
    ".db",
    ".pem",
    ".key",
    ".pfx",
    ".p12",
    ".crt",
    ".cer",
    ".der",
    ".csv",
    ".tsv",
    ".jsonl",
    ".zip",
    ".whl",
}


def intended_files() -> list[str]:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 0 and Path(result.stdout.strip()).resolve() == ROOT:
        listing = subprocess.run(
            [
                "git",
                "-C",
                str(ROOT),
                "ls-files",
                "--cached",
                "--others",
                "--exclude-standard",
                "-z",
            ],
            capture_output=True,
            check=True,
        )
        return sorted({name for name in listing.stdout.decode().split("\0") if name})
    return sorted(
        path.relative_to(ROOT).as_posix()
        for path in ROOT.rglob("*")
        if path.is_file() and not any(part in IGNORED for part in path.relative_to(ROOT).parts)
    )


def findings(relative: str, text: str) -> list[str]:
    issues = []
    path = Path(relative)
    if any(part in IGNORED for part in path.parts):
        issues.append("generated/private directory selected for publication")
    if path.suffix.lower() in BLOCKED_EXTENSIONS and relative != ALLOWED_CSV:
        issues.append("operational data, binary artifact or credential material")
    if path.name == ".env" or path.name.startswith(".env.") or path.suffix == ".env":
        issues.append("environment/secret file")
    checks = {
        "private-key content": r"-----BEGIN [A-Z ]*PRIVATE KEY-----",
        "access token": r"\b(?:gh[pousr]_[A-Za-z0-9]{30,}|github_pat_[A-Za-z0-9_]{30,}|AKIA[A-Z0-9]{16})\b",
        "embedded URL credentials": r"(?:https?|opc\.tcp)://[^\s/:]+:[^\s/@]+@",
        "personal absolute path": r"(?:[A-Za-z]:[\\/]Users[\\/]|/home/)[A-Za-z0-9_.-]+",
        "private device address": r"\b(?:192\.168\.\d{1,3}\.\d{1,3}|10\.\d{1,3}\.\d{1,3}\.\d{1,3}|172\.(?:1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b",
    }
    for label, pattern in checks.items():
        if re.search(pattern, text):
            issues.append(label)
    if (
        path.suffix in {".yaml", ".yml"}
        and relative.startswith("config/")
        and re.search(r"(?im)^\s*(?:host|endpoint|sender|username):\s*(?!null\s*$|#|$)\S+", text)
    ):
        issues.append("live-looking integration setting in committed config")
    return issues


def main() -> None:
    files = intended_files()
    repository = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--show-toplevel"],
        capture_output=True,
        text=True,
        check=False,
    )
    own_repository = (
        repository.returncode == 0 and Path(repository.stdout.strip()).resolve() == ROOT
    )
    failures = []
    for relative in files:
        path = ROOT / relative
        if path.is_symlink():
            failures.append((relative, "symlink requires explicit review"))
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeError, OSError):
            failures.append((relative, "unreadable or binary file"))
            continue
        failures.extend((relative, issue) for issue in findings(relative, text))
        if own_repository:
            staged = subprocess.run(
                ["git", "-C", str(ROOT), "show", f":{relative}"], capture_output=True, check=False
            )
            if staged.returncode == 0:
                try:
                    staged_text = staged.stdout.decode("utf-8")
                except UnicodeError:
                    failures.append((relative, "binary content in Git index"))
                else:
                    failures.extend(
                        (relative, "staged: " + issue) for issue in findings(relative, staged_text)
                    )
    if failures:
        for relative, issue in failures:
            print(f"REVIEW {relative}: {issue}")
        raise SystemExit(1)
    print(
        f"Publication file checks passed: {len(files)} text files; manual content/permission review still required."
    )


if __name__ == "__main__":
    main()
