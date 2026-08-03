# Scope and limits of this measurement

**Date:** 2026-07-31 · **Subject:** ThingsFlow **only**

## What it measures

ThingsFlow's resource footprint across four regimes — `idle`, `light` (50 devices),
`moderate` (200) and `heavy` (500) — with per-pod CPU and memory sampling, NATS stream
depth and host CPU.

## What it does NOT measure, and why that matters

**It does not verify that the data landed.** No scenario counts rows in the store to
confirm that what was sent was persisted. A resource measurement without loss
verification cannot distinguish "the system processed everything with X CPU" from "the
system dropped part of it and that is why it spent X CPU".

At idle and under light load that is of little consequence — there is little or nothing to
lose — but it means that **the `moderate` and `heavy` scenarios are not capacity claims**.
They must not be cited as "ThingsFlow sustains N devices".

**It is not comparative.** There is no ThingsBoard here. Any comparison needs the ramp in
`benchmarks/results-fair/`, which does equalize budgets, verifies landing, and checks that
no CPU limit bound during the measured window.

## What it is good for, then

As a baseline of the footprint at idle and under light load, which is what it measures
well. That figure is solid: at idle there is no traffic to lose, so the absence of
verification does not compromise it.

## Context

It was taken before `throttle-gate.py` (CFS throttling check) and the isolation between
levels existed. The method defects that were corrected afterwards are documented in
`benchmarks/METHODOLOGY.md`.
