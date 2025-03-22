
# duckdb.yazi

[duckdb](https://github.com/duckdb/duckdb) now in [yazi](https://github.com/sxyazi/yazi). 


<img width="1710" alt="Screenshot 2025-03-22 at 17 59 21" src="https://github.com/user-attachments/assets/ac006667-4281-4e0a-87a4-bfaeefc6f20b" />

## Installation

To install, use the command 

    ya pack -a wylie102/duckdb

and add to your `yazi.toml`:

```toml
[plugin]
prepend_previewers = [
  { name = "*.csv", run = "duckdb" },
  { name = "*.json", run = "duckdb" },
  { name = "*.parquet", run = "duckdb" },
  { name = "*.tsv", run = "duckdb" },
  { name = "*.xlsx", run = "duckdb" }
]
```

## Dependencies

### Yazi
[Installation installations](https://yazi-rs.github.io/docs/installation)



### duckdb
[Installation instructions](https://duckdb.org/docs/installation/?version=stable&environment=cli&platform=macos&download_method=direct)

## Recommended plugins

I recommend using this with a larger size for your preview window or using the maximise preview pane plugin:

<https://github.com/yazi-rs/plugins/tree/main/toggle-pane.yazi>

## What does it do?

Calls duckdb's summarize function to preview your data files.
(Note that the display is not exactly like summarize as I added in some SQL CASE statements to make it more human readable and truncated some column outputs, the goal being to fit more in the preview window.)

Can be used on:

- .csv
- .json
- .parquet
- .tsv
- .xlsx

Can also be used to preview files in standard format, rather than summarized.

To change the mode, from the command line run:

Summarized will display the default output summarizing each column as a row.


    export DUCKDB_PREVIEW_MODE=summarized


Standard returns a standard view of your file as if it were a table.


    export DUCKDB_PREVIEW_MODE=standard


Standard returns a standard view of your file as if it were a table.

Both views are vertically scrollable with J and K.


Currently scrolling in parquet in standard will perform the best, ans summarized the slowest as the query is re-run each time to move the output, but I am working on implementing caching to speed this up.

## Preview

<img width="1710" alt="Screenshot 2025-03-22 at 18 00 06" src="https://github.com/user-attachments/assets/db09fff9-2db1-4273-9ddf-34d0bf087967" />
