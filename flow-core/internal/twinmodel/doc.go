// Package twinmodel defines the pure, persistence-independent ThingsFlow twin
// model language and its state validator.
//
// Authored models use a DTDL-shaped envelope whose property constraints are a
// deliberately small JSON Schema subset. Normalize turns that authored JSON
// into a deterministic DerivedSchema while retaining unrelated authored
// metadata only on Model. Model IDs are stored as bare slugs; consumers compose
// definition URNs.
//
// Validate is null-tolerant for defensive interoperability and partial merges.
// The current SEM producer emits numbers, not null. Required constraints apply
// only to complete-document validation. UnknownKeys applies inside modeled
// features and never to the catch-all telemetry feature.
//
// Phase 2 Plan 02-04 enforces classic attribute writes with this package.
// Phase 3 feature writers must reuse the same enforcer; reject mode remains an
// explicit opt-in while warn is the default.
package twinmodel
