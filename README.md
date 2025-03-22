
# duckdb.yazi

[duckdb](https://github.com/duckdb/duckdb) now in [yazi](https://github.com/sxyazi/yazi). To install, use the command `ya pack -a wylie102/duckdb.yazi` and add to your `yazi.toml`:

```toml
[plugin]
prepend_previewers = [
  { name = "*.csv", run = "duckdb" },
  { name = "*.json", run = "duckdb" },
  { name = "*.parquet", run = "duckdb" },
  { name = "*.tsv", run = "duckdb" },
  { name = "*.xlsx", run = "duckdb" },
```

## Recommended plugins

I recommend using this with a larger size for your preview window or using the maximise preview pane plugin:

<https://github.com/yazi-rs/plugins/tree/main/toggle-pane.yazi>

## What does it do?

Calls duckdb's summarize function to preview your data files.
(Note that the display is not exactly like summarize as I added in some SQL CASE statements to make it more human readable and truncated some column outputs, the goal being to fit more in the preview window.)

Can be used on:
.csv
.json
.parquet
.tsv
.xlsx

## Preview
