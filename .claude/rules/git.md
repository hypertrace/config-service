# Git Workflow

- **Branch naming**: `JIRA-ID-description` or `NO-TICKET-description` (e.g., `AAP-1234-add-fraud-config`, `NO-TICKET-fix-typo`)
- **Commit format**: `JIRA-ID: Description` or `NO-TICKET: Description`
- **PR title format**: Same as commit format — follow the established pattern
- **Default branch**: `main`
- **Git submodules**: This repo includes `hypertrace-config-service` as a git submodule. Always use `--recursive` flag when cloning or updating
- **Breaking proto changes**: Any PR that modifies `.proto` files in a breaking way must justify it — `buf breaking` runs automatically on every PR
- **Wait for CI**: All checks must pass before merge — build, test, proto validation, helm chart validation, OWASP dependency check, security scans

## CRITICAL: Never Push Directly to Main

**NEVER push directly to main**, even if you have permissions that allow bypassing branch protection rules.

- DO NOT run `git push origin main`
- DO NOT bypass branch protection
- DO NOT merge locally and push to main
- ALWAYS create a branch, push the branch, and create a PR
- ALWAYS go through the PR review process
- ALWAYS wait for CI/CD checks and approvals

**If you accidentally push to main:**
1. Immediately notify the team
2. Create a follow-up PR documenting the changes
3. Consider reverting if changes are breaking or untested

## Git Submodules

This repository includes `hypertrace-config-service` as a git submodule:

- **Clone with submodules**: `git clone --recursive <repo-url>`
- **Update submodules**: `git submodule update --init --recursive`
- **CI validation**: CI enforces that submodules point to commits from the main branch
- **When updating submodules**: Ensure the commit is from the main branch of the submodule repository
