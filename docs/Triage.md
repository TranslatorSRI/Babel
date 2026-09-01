# Babel Issue Triage

This document is a guide for Babel developers and project managers triaging and prioritizing
issues in the [Babel issue tracker] using the [Babel sprints GitHub project].

For guidance on how to file an issue — including what to include, how to fill in priority,
impact and size, and how to track when your issue will be addressed — see
[docs/NewIssue.md](./NewIssue.md).

---

## Triage guide (for developers)

### Triage checklist

When a new issue arrives, work through the following steps:

1. **Reproduce or understand the report.** Read the issue carefully. Can the problem be confirmed
   from the description? If not, ask the reporter for more information (e.g. which Babel build
   they are using, example identifiers).

2. **Check for duplicates.** Search for existing issues that describe the same problem. If a
   duplicate exists, close the new issue with a reference to the original (or add the new issue
   as a sub-issue of the original).

3. **Set Priority, Impact and Size.** If the reporter has filled these in, review them and adjust
   if necessary. If they are blank, set them now based on your assessment. Don't be shy about
   changing them in the future if necessary.

4. **Set the Component field.** Choose the appropriate **Component** value (see the Component
   table in [docs/NewIssue.md](./NewIssue.md#grouping-related-issues)) if that would be useful.
   This can help group together related issues and for filtering during sprint planning.

5. **Link to a parent issue.** If this issue is one instance of a broader known problem (e.g. a
   deprecated identifier source, or a class of missing cliques), set the **Parent issue** field.

6. **Set Status to Backlog.** Unless the issue is Critical and needs immediate scheduling, move
   it to the **Backlog** column.

7. **Add an automated test.** See the next section for how to embed tests directly in the issue
   description.

### Adding automated tests to issues

The [babel-validation] project can run automated checks against live NodeNorm and NameRes
instances that are triggered by issues. You can embed tests directly in issue descriptions or
comments using two syntaxes.

#### Wiki syntax (single assertion)

```text
{{BabelTest|AssertionType|param1|param2|...}}
```

For example, to assert that two CURIEs resolve to the same clique:

```text
{{BabelTest|ResolvesWith|MESH:D014867|DRUGBANK:DB09145}}
```

#### YAML syntax (multiple assertions)

Use a fenced code block with the language tag `yaml` and a top-level property `babel_tests`:

````text
This can be inserted anywhere in the issue.

```yaml
babel_tests:
  ResolvesWith:
    - ['MESH:D014867', 'DRUGBANK:DB09145']
  HasLabel:
    - ['MESH:D014867', 'Water']
```

Having text after the fenced code block (or multiple code blocks) is fine too.
````

#### Available assertion types

The assertion types are defined in [babel-validation][assertion types], whose README is generated
from the handler classes themselves. That page is canonical: it is always in sync with what the
harness can actually run, so link to it rather than copying the list here. As of writing it covers
`Resolves`, `DoesNotResolve`, `ResolvesWith`, `DoesNotResolveWith`, `HasLabel` and
`ResolvesWithType` for NodeNorm, `SearchByName` for NameRes, and the `Needed` placeholder.

Use `{{BabelTest|Needed}}` as a placeholder if you know a test is needed but do not yet know the
exact expected values. It always fails, as a reminder.

An assertion type that the harness does not recognise is a hard failure regardless of whether the
issue is open or closed, so a typo or an invented name takes down the whole run rather than
failing quietly. This document used to carry its own copy of the assertion table, and that copy
drifted: the preferred-name issue template pre-filled a `HasPreferredName` assertion that has
never existed. Check a name against the generated README rather than against prose.

### Sprint planning

Sprints are two weeks long. At the start of each sprint:

1. **Carry over unfinished items.** Any issues still **In progress** or **Ready** that were not
   completed automatically move to the next sprint.

2. **Review the backlog.** Sort the backlog by Priority (descending) then Impact (descending).
   Consider Size to avoid overloading a sprint — a sprint full of XL issues will not complete on
   time.

3. **Select issues for the sprint.** Choose the highest-priority issues that together represent a
   realistic amount of work for two weeks. Move selected issues to **Ready**.

4. **Adjust if needed.** An issue may be removed from the current sprint mid-sprint if it turns
   out to be much larger than estimated, or if a Critical issue arrives that must take precedence.
   In either case, the deferred issue should be the first candidate for the next sprint.

#### Heuristics for issue selection

- Prefer **Critical** issues regardless of impact.
- Among **High** and **Medium** priority issues, prefer those with **Enormous** or **High** impact.
- Prefer **XS** and **S** issues when a sprint already contains several large items — clearing
  small issues reduces backlog pressure.
- Issues with automated tests (see above) are easier to verify once fixed; prefer these when all
  else is equal.
- Group issues sharing the same **Parent issue** or **Component** — fixing a class of bugs together
  is more efficient than fixing them one at a time across different sprints.

[Babel issue tracker]: https://github.com/NCATSTranslator/Babel/issues/
[Babel sprints GitHub project]: https://github.com/orgs/NCATSTranslator/projects/36
[babel-validation]: https://github.com/TranslatorSRI/babel-validation
[assertion types]: https://github.com/TranslatorSRI/babel-validation/blob/main/src/babel_validation/assertions/README.md
