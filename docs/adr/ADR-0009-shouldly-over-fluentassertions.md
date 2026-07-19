# ADR-0009 — Shouldly instead of FluentAssertions (assertion-library licence risk)

- **Status:** accepted
- **Date:** 2026-07-19
- **Deciders:** Grigoris Zapantis

## Context

The test stack (MASTER-PLAN E12) named **FluentAssertions**. In January 2025 FluentAssertions v8 was
released under a proprietary licence following an Xceed partnership:

- **v8+** — free for non-commercial use only; **commercial use requires a paid licence** (~$130 per
  developer per year).
- **v7** — remains Apache-2.0 **indefinitely**, but explicitly receives **critical fixes only**; no
  further feature development.

A Dependabot PR proposing `6.12.2 → 8.10.0` forced the decision.

The deciding detail: **Xceed's public FAQ does not define "commercial" vs "non-commercial"** — it
defers to a separate licence agreement. This repository is MIT-licensed and public, which likely
qualifies for the free tier, but a *portfolio* exists to attract commercial work, so relying on an
undefined boundary is an avoidable risk.

Timing also mattered. The suite has **82 assertions across 9 files** today; MASTER-PLAN Week 4 targets
80% coverage, which multiplies that. This was the cheapest moment to move.

## Decision

**Adopt [Shouldly](https://github.com/shouldly/shouldly) (MIT) as the assertion library.**

Two alternatives were seriously considered:

| Option | Why not chosen |
|---|---|
| **Pin FluentAssertions v7** | Free forever, zero effort — but a frozen, critical-fixes-only dependency. Defers the decision rather than resolving it. |
| **AwesomeAssertions** (community Apache-2.0 fork of FA v7, near drop-in) | Genuinely good, and the cheapest migration. Rejected because it is a fork whose existence is defined by another project's licensing move, and its maintainers state it has **no commercial entity backing it**. |
| **Shouldly** ✅ | MIT, independently governed, established, actively maintained. Its roadmap does not depend on what Xceed does next. |

### Why the usual objection to Shouldly does not apply here

Shouldly is normally dismissed for lacking `AssertionScope` and having weaker deep object-graph
comparison than `BeEquivalentTo`. Auditing actual usage before deciding showed neither is a
constraint for this codebase:

- **`AssertionScope`: zero uses.**
- **`BeEquivalentTo`: 3 uses**, all *collection* comparisons (asserting a JSON payload's property
  names match the ClickHouse columns) — not object-graph diffing. These map directly to
  `ShouldBe(expected, ignoreOrder: true)`.

The remaining ~79 assertions are `Be`/`BeTrue`/`Contain`/`ContainKey`/`BeEmpty`/`Throw`, all of which
have one-to-one Shouldly equivalents.

### Migration notes

| FluentAssertions | Shouldly |
|---|---|
| `x.Should().Be(y)` | `x.ShouldBe(y)` |
| `x.Should().BeTrue()` / `BeFalse()` / `BeNull()` | `x.ShouldBeTrue()` / `ShouldBeFalse()` / `ShouldBeNull()` |
| `x.Should().Contain(y)` / `ContainKey(k)` | `x.ShouldContain(y)` / `ShouldContainKey(k)` |
| `x.Should().BeEmpty()` / `NotBeEmpty()` | `x.ShouldBeEmpty()` / `ShouldNotBeEmpty()` |
| `x.Should().OnlyHaveUniqueItems()` | `x.ShouldBeUnique()` |
| `x.Should().HaveCount(n)` | `x.Count.ShouldBe(n)` |
| `x.Should().BeEquivalentTo(items)` | `x.ShouldBe(items, ignoreOrder: true)` |
| `xs.Should().Contain([a, b, c])` | `new[]{a,b,c}.ShouldBeSubsetOf(xs)` |
| `act.Should().Throw<T>()` | `Should.Throw<T>(() => act())` |
| `act.Should().Throw<T>().WithMessage("*frag*")` | `Should.Throw<T>(() => act()).Message.ShouldContain("frag")` |

Shouldly is pinned at **4.3.0** — the current release. (Shouldly 5, which drops dependencies and adds
Native AOT support, is in development but not yet published to NuGet.)

## Consequences

- **Positive:** the licence question is closed permanently — MIT, no commercial tier, no
  "non-commercial" boundary to interpret. Done at 82 assertions rather than several hundred.
- **Negative:** a one-time rewrite of every assertion, and the loss of `AssertionScope` /
  `BeEquivalentTo` should the suite later want deep object-graph comparison. If that need arises,
  a focused structural-comparison helper is preferable to re-introducing a licensed dependency.
- **Verification:** beyond the suite passing, the migration was **mutation-checked** — deliberately
  corrupting an expected value and a collection member made the tests fail, confirming the rewritten
  assertions still assert rather than silently passing.
- **Canon:** MASTER-PLAN **E12** updated (the test stack now reads *xUnit + Shouldly + NetArchTest +
  Testcontainers*). Dependabot PR #7 (`FluentAssertions 6.12.2 → 8.10.0`) is closed as superseded.
