# Local validation

Validated on 25 October 2026 using Python 3.12 on Windows. All fixtures and demonstration
readings were constructed in code; no operational dataset or external machine was used.

- 107 regression tests passed, including four publication-guardrail tests.
- Ruff lint and formatting checks passed.
- Strict Mypy checks passed for the renamed application package.
- The offline demo stored 120 synthetic cycles and reconciled all 120 in a separate receiving
  database. A workbook and cycle export were generated and validated; duplicate ingestion was checked.
- The demo used fictional machine/job/product/material identifiers and sent no email.

These results establish local software behaviour. The GitHub Actions workflow still needs to
execute after upload, and no physical edge-device, UPS or production deployment claim is made.

Reproduce from the repository root using the commands in README. Generated files are ignored
and are deliberately excluded from the upload archive.
