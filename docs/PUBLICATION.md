# Preparing a public repository

Commit source, constructed tests, generic templates and synthetic examples only. Do not add
live configuration, device addresses, machine exports, reports, databases, secrets, certificates,
personal paths or real job/material identifiers.

Run `python tools/check_publication.py` and review `git diff --cached` before committing.
Automated checks are guardrails, not proof of publication clearance. No open-source licence
has been selected. Confirm that you are entitled to publish all included material before making
the repository public or choosing a licence; removing identifiers does not determine ownership.

## Upload using your own account

Create an empty repository named `industrial-edge-process-logger` in your GitHub account.
Use a personal Git identity or your GitHub-provided no-reply address. Replace the placeholders
below before running the commands. Setting a local identity avoids inheriting a work identity.
From this directory (the first command also prepares an extracted ZIP):

```shell
git init --initial-branch=main
git config --local user.name "YOUR_PUBLIC_NAME"
git config --local user.email "YOUR_GITHUB_NOREPLY_EMAIL"
git status
python tools/check_publication.py
git add .
git diff --cached --stat
git commit -m "Initial synthetic industrial edge logger showcase"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/industrial-edge-process-logger.git
git push -u origin main
```

Upload this repository's contents only. Its parent directory is not part of the repository.
Start with fresh history and keep generated outputs in ignored `build/` directories. The hosted
CI configuration is not evidence of a successful CI run until it actually executes on GitHub.
