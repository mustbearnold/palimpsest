# Security policy

## Reporting a vulnerability

Do not open a public issue for a vulnerability, credential, cross-tenant memory
exposure, deletion failure, or private-data leak. Use GitHub's private security
advisory reporting for this repository.

Include the affected version or commit, impact, reproduction steps, and any
suggested mitigation. Do not include real customer memory or active credentials.

## Supported versions

Palimpsest has not released a supported production version. Security fixes will
target the default branch until a version-support policy is published.

## Security boundaries

Tenant authorization, subject scope, deletion, provenance, temporal validity,
and retrieval-filter ordering are security behavior. Changes to those surfaces
require explicit tests and independent review.
