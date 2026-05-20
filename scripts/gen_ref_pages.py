"""Auto-generate API reference pages for mkdocstrings.

Walks the ``aiokpl/`` package, emits one Markdown stub per public module
under ``docs/reference/``, and produces a literate-nav ``SUMMARY.md`` so the
``API reference`` section of the nav fills itself in.

Private modules (those whose name starts with ``_``) and ``__init__`` /
``__main__`` are skipped — they show up as the package root instead.
"""

from __future__ import annotations

from pathlib import Path

import mkdocs_gen_files

PACKAGE = "aiokpl"
SRC_ROOT = Path(__file__).resolve().parent.parent
PKG_ROOT = SRC_ROOT / PACKAGE
REF_ROOT = Path("reference")

nav = mkdocs_gen_files.Nav()

for path in sorted(PKG_ROOT.rglob("*.py")):
    module_path = path.relative_to(PKG_ROOT).with_suffix("")
    parts = (PACKAGE, *module_path.parts)

    if parts[-1] == "__init__":
        parts = parts[:-1]
        doc_parts = parts
        if not doc_parts:
            continue
    elif parts[-1].startswith("_"):
        continue
    else:
        doc_parts = parts

    if any(p.startswith("_") for p in parts):
        continue

    doc_path = REF_ROOT / Path(*doc_parts).with_suffix(".md")
    full_doc_path = doc_path

    ident = ".".join(parts)
    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        fd.write(f"# `{ident}`\n\n::: {ident}\n")

    nav[doc_parts] = doc_path.relative_to(REF_ROOT).as_posix()

with mkdocs_gen_files.open(REF_ROOT / "SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
