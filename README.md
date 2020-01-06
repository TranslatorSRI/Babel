# Babel

Babel integrates multiple naming systems, creating equivalent sets across multiple semantic types.  Each semantic type (such as chemical substance) requires specialized processing, but in each case, a JSON-formatted compendium is written to disk.

Once these files are created (and they may be created by means other than the included scripts), they are loaded into redis using the scheme described in R3, and can be served via that tool.
