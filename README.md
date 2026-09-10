# SQLite driver for Go `database/sql`

Work in progress. Nothing to see here.

## Updating SQLite

The script `update-sqlite.sh` at the top of the repository partially automates
this process. It accepts a SQLite amalgamation URL as the first argument, or
otherwise uses the URL recorded in [version-url.txt](./version-url.txt):

```
./update-sqlite.sh https://sqlite.org/2024/sqlite-amalgamation-3460100.zip
```
