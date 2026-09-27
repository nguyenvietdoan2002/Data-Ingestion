"""
Convert subtitle files (.vtt / .srt) to plain text (.txt).

Default input: data-lake/bronze/youtube/ (searches recursively for .vtt/.srt)
Output: sibling text/ directory next to each subs/ directory.

Usage:
  python ingestion/youtube/script_to_text.py
  python ingestion/youtube/script_to_text.py --input data-lake/bronze/youtube/year=.../subs
  python ingestion/youtube/script_to_text.py --input path/to/file.vtt --output path/to/out.txt
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_INPUT = _REPO_ROOT / "data-lake" / "bronze" / "youtube"

_INLINE_TS = re.compile(r"<\d{2}:\d{2}(?::\d{2})?(?:\.\d+)?>")
_HTML_TAG = re.compile(r"<[^>]+>")


def _clean_caption_line(line: str) -> str:
    line = _INLINE_TS.sub("", line)
    line = _HTML_TAG.sub("", line)
    return " ".join(line.split()).strip()


def _is_timestamp_line(line: str) -> bool:
    return "-->" in line


def _is_header_line(line: str) -> bool:
    s = line.strip()
    if not s:
        return True
    if s == "WEBVTT":
        return True
    if s.startswith("Kind:") or s.startswith("Language:"):
        return True
    if s.startswith("NOTE"):
        return True
    return False


def parse_subtitle_text(content: str) -> list[str]:
    """Extract readable caption lines from VTT or SRT content."""
    lines_out: list[str] = []
    prev: str | None = None

    for raw in content.splitlines():
        s = raw.strip()
        if _is_header_line(s) or _is_timestamp_line(s):
            continue
        if s.isdigit():
            continue

        text = _clean_caption_line(s)
        if not text:
            continue
        if text == prev:
            continue

        lines_out.append(text)
        prev = text

    return lines_out


def subtitle_to_txt(content: str) -> str:
    return "\n".join(parse_subtitle_text(content))


def video_id_from_sub_path(path: Path) -> str:
    # e.g. 1r5fBgzpKOI.en.en.vtt -> 1r5fBgzpKOI
    return path.name.split(".", 1)[0]


def default_output_path(sub_path: Path) -> Path:
    # .../subs/foo.vtt -> .../text/foo.txt
    day_dir = sub_path.parent.parent
    text_dir = day_dir / "text"
    return text_dir / f"{video_id_from_sub_path(sub_path)}.txt"


def iter_subtitle_files(root: Path) -> list[Path]:
    if root.is_file():
        return [root] if root.suffix.lower() in {".vtt", ".srt"} else []
    return sorted(
        p
        for p in root.rglob("*")
        if p.is_file() and p.suffix.lower() in {".vtt", ".srt"}
    )


def convert_file(sub_path: Path, output_path: Path | None = None) -> Path:
    out = output_path or default_output_path(sub_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    text = subtitle_to_txt(sub_path.read_text(encoding="utf-8", errors="replace"))
    out.write_text(text + ("\n" if text else ""), encoding="utf-8")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Convert subtitle files to plain .txt")
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Subtitle file or directory (default: data-lake/bronze/youtube/)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output .txt file (only when --input is a single file)",
    )
    args = parser.parse_args()

    input_path: Path = args.input if args.input.is_absolute() else (Path.cwd() / args.input).resolve()

    if args.output and input_path.is_dir():
        print("Error: --output only applies when --input is a single subtitle file.")
        return 2

    files = iter_subtitle_files(input_path)
    if not files:
        print(f"No .vtt/.srt files found under: {input_path}")
        return 1

    for sub_path in files:
        out_path = convert_file(sub_path, args.output if input_path.is_file() else None)
        print(f"Wrote: {out_path}")

    print(f"Done. Converted {len(files)} file(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
