# GitHub Copilot Contribution Rules

One page. No ambiguity. Follow or reject the change.

## Good engineering
- always prefer to remove code than to add it
- always look for ways to simplify

## Core
- Remove things completely. No placeholders, stubs, TODO, commentary about removal. History lives only in git.
- No historical or refactor narration comments. Comments only explain current behavior, invariants, limits, dialect nuances, or (released) API deprecations.
- ASCII only. No Unicode of any kind.

## Public API
- Public = exported + existed in a tagged release + intentionally documented.
- Changing/removing: update doc comment only if callers must adapt.
- Shim only if break is not trivially fixable. Shim must be minimal, delegated, marked `// Deprecated:` with planned removal version, and tested.
- No shims for anything else.

## Testing
- Coverage target 95-100% for unit-testable code. 
- No mocks.
- Use testify.
- Deterministic, table-driven tests. Assert meaningful outcomes only.
- Disallow: mocking frameworks, log-text assertions (unless log text is contract), sleeps for timing.
- Prefer errors.Is() over checking error text

## Style & Refactors
- Prefer elimination over abstraction. No commented-out code. No speculative feature flags.
- Inline vs extract based strictly on clarity or real reuse.
- Silent refactors do not add commentary. Behavioral changes only update directly affected docs.

## Errors
- Use memsql/errors only.
- Sentinel errors with errors.String type, augmented with .Errorf
- Wrap once at subsystem boundaries with context. Do not re-wrap in tight loops. Avoid repeating obvious parameter values.
- Don't capitalize the first letter

## Temporary changes for debugging
- Mark temporary code with "XXX"
- Add freely

## Checklist (all must be true)
- [ ] No history / refactor narration comments.
- [ ] No placeholders or commented-out code left behind.
- [ ] ASCII only.
- [ ] Public API changes reviewed; shims only when allowed & tested.
- [ ] Coverage for unit-testable code >=95% 
- [ ] No mocks.
- [ ] Error wrapping matches boundary rule.
- [ ] Tests use testify and do not use t.Errorf() or t.FailNow()
- [ ] Sentinel errors use errors.String
- [ ] No temporary (XXX) code remaining for commits

## Enforcement
Non-compliant changes are rejected regardless of technical merit.
