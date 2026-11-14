# Record architecture decisions

- Status: Accepted
- Date: 2026-11-13
- Deciders: Nicolás Hermosilla
- Approved by: Nicolás Hermosilla

## Context
The project is a reimplementation driven by a legacy gateway. Requirements, decisions and delivered
behaviour must stay traceable and current so that humans and agents can work from them.

## Decision
Use the `general-software-development` workflow: a living spec in `docs/spec/`, ADRs in `docs/adr/`,
intents and slices in `docs/branch/` while a branch is open, and vertical slices each proven by an
end-to-end test. The skill is vendored under `.agents/skills/general-software-development/`.

## Alternatives considered
- Ad-hoc documentation — drifts from the code and loses the reasons for decisions.

## Consequences
Every non-trivial change needs approved records first. The merge gate script checks the form of the records.
