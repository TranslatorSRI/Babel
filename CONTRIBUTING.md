# Contributing to Babel

Babel is open-source software and all contributions are very welcome!

## Reporting bugs

Reporting when you see something wrong with Babel is very helpful, whether you spot it in the
Babel output files or through one of the frontends. Following these guidelines will help you
submit the most useful bug reports and will help us triage and prioritize them correctly.

1. Is the issue related to the [Node Normalizer] application, such as invalid output, an unexpected
   error message, mishandling input, or something else? If so, please report them in the
   [Node Normalizer issue tracker](https://github.com/TranslatorSRI/NodeNormalization/issues/).
2. Is the issue related to the [Name Resolver] application, such as invalid output, an unexpected
   error message, mishandling input, or search results not being ranked correctly? If so, please
   report them in the [Name Resolver issue tracker](https://github.com/TranslatorSRI/NameResolution/issues/).
3. Most other issues will be [Babel issues](https://github.com/TranslatorSRI/Babel/issues/), in particular
   anything to do with cliques whose identifiers, Biolink type, preferred label, other labels, synonyms or
   descriptions are incorrect. If you're not sure about which repository your issue should go to, please
   add it to Babel and we'll sort it out at our end.
4. When reporting the bug, providing a link to the NodeNorm clique or NameRes query that shows the incorrect
   output will be very helpful. We would also appreciate if you can include what you expect the tool to return.
5. After you have reported a bug, helping to triage, prioritize and group it will be very helpful:
    - We triage issues into one of the [milestones](https://github.com/TranslatorSRI/Babel/milestones):
      - [Needs investigation](https://github.com/TranslatorSRI/Babel/milestone/12) refers to issues that need
        to be investigated further -- either to figure out what is causing the issue or to communicate with the
        user community to understand what should occur.
      - [Immediate](https://github.com/TranslatorSRI/Babel/milestone/35) need to be fixed immediately. Issues I'm
        currently working on will be placed here.
      - [Needed soon](https://github.com/TranslatorSRI/Babel/milestone/30) refers to issues that should be fixed
        in the next few months: not immediately, but sooner rather than later.
      - [Needed later](https://github.com/TranslatorSRI/Babel/milestone/31) refers to issues that should be fixed
        eventually, but are not needed immediately.
      - [Needs testing](https://github.com/TranslatorSRI/Babel/milestone/27) refers to issues that need additional
        testing, such as determining how widely an issue occurs, whether an issue is reproducible or -- in some cases --
        to test whether a particular PR has in fact fixed the submitted issue.
    - We prioritize issues with one of the three priority tags: [Priority: Low](https://github.com/TranslatorSRI/Babel/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22Priority%3A%20Low%22),
      [Priority: Medium](https://github.com/TranslatorSRI/Babel/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22Priority%3A%20Medium%22),
      [Priority: High](https://github.com/TranslatorSRI/Babel/issues?q=is%3Aissue%20state%3Aopen%20label%3A%22Priority%3A%20High%22).
      The idea is that issues with the highest priority will determine which will be investigated/tested first, and which
      are most likely to move from Needed later/Needed soon into Immediate for working on.
    - You can group issues in two ways:
      - GitHub lets you chose a "parent" issue for each issue, which is useful for issues that are related to each
        other. We try to build "issues of issues" that group together similar issues that might require similar fixes
        (e.g. [our issue tracking deprecated identifiers](https://github.com/TranslatorSRI/Babel/issues/93)). If you
        find an issue related to yours, please feel free to add yours as a child of the existing issue or vice versa.
      - You can use labels to group similar issues. We don't have a lot of labels for you to choose from, but feel free
        to add any that make sense!

## Writing code

Babel is structured around its [Snakemake files](./src/snakefiles), which call into its
[data handlers](./src/datahandlers) and [compendia creators](./src/createcompendia). The
heart of its data are its concord files, which contain cross-references between different
databases. These are combined into compendium files and synonyms.

### Writing a new concord or compendium
TBD

### Adding a new source of identifiers, synonyms or descriptions 
TBD

### Submitting a PR
TBD

## Want to work on the frontends instead?

Babel has two frontends: the [Node Normalizer] for
exposing information about cliques, and the [Name Resolver],
which lets you search by synonyms or names.

- [Node Normalizer]: https://github.com/TranslatorSRI/NodeNormalization
- [Name Resolver]: https://github.com/TranslatorSRI/NameResolution
