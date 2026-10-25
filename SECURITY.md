# Security and operational boundaries

- Read-only machine integration; no write/control API in the logger.
- Loopback simulator and synthetic demo are development tools, not production services.
- TLS, authentication and certificate trust require site-specific configuration.
- Secrets belong in environment variables, never Git.
- Keep operational data and identifiers out of commits; generated artifacts are ignored.
- UPS shutdown and network templates require operator review before installation.

Do not post secrets or operational data in public issues. Use a minimal synthetic reproduction
when reporting a defect and remove environment-specific details.
