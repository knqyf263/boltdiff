# boltdiff

A command-line tool to compare two BoltDB databases and show the differences.

![usage](./imgs/usage.png)

## Installation

```bash
go install github.com/knqyf263/boltdiff@latest
```

## Usage

```bash
boltdiff [options] DB1 DB2
```

## Options

| Option | Description |
|--------|-------------|
| `-bucket value` | Bucket path to compare (can be specified multiple times for nested buckets) |
| `-exclude-pattern string` | Exclude keys matching the regex pattern |
| `-key-only` | Show only key names without value diffs |
| `-raw` | Print raw bytes instead of strings |
| `-skip-added` | Suppress added keys |
| `-skip-deleted` | Suppress deleted keys |
| `-skip-modified` | Suppress modified items |
| `-summary` | Print only summary counts |
| `-verbose` | Print verbose logs |

## Examples

### Basic comparison

```bash
boltdiff db1.db db2.db
```

Output:
```
Deleted: 1
--- bucket1 -> key4

Added: 1
+++ bucket1 -> key5

Modified: 1
diff a/db1.db b/db2.db
--- a/bucket1 -> key3
+++ b/bucket1 -> key3
  string(
-     "old value",
+     "new value",
  )
```

### Compare specific bucket

```bash
boltdiff -bucket mybucket db1.db db2.db
```

### Compare nested bucket

```bash
boltdiff -bucket parent -bucket child db1.db db2.db
```

### Show only key names (no value diffs)

```bash
boltdiff -key-only db1.db db2.db
```

Output:
```
Deleted: 1
--- bucket1 -> key4

Added: 1
+++ bucket1 -> key5

Modified: 1
*** bucket1 -> key3
```

### Show only summary

```bash
boltdiff -summary db1.db db2.db
```

Output:
```
Deleted: 1

Added: 1

Modified: 1
```

### Show only deleted keys

```bash
boltdiff -skip-added -skip-modified db1.db db2.db
```

### Exclude keys by pattern

```bash
boltdiff -exclude-pattern "^temp.*" db1.db db2.db
```

### Verbose output

```bash
boltdiff -verbose db1.db db2.db
```

## Key Format

Keys are displayed in the format:
```
bucket1 -> bucket2 -> ... -> key
```

For nested buckets, each level is separated by ` -> `.

## License

MIT
