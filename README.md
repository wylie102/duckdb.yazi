# duckdb.yazi

**Uses  [duckdb](https://github.com/duckdb/duckdb) to quickly preview and summarize data files in [yazi](https://github.com/sxyazi/yazi)!**

https://github.com/user-attachments/assets/d4a4e944-4119-4197-8e10-195520f364be

## What does it do?

This plugin previews your data files in yazi using DuckDB, with two available view modes:

- Standard mode (default): Displays the file as a table
- Summarized mode: Uses DuckDB's summarize function, enhanced with custom formatting for readability
- Scroll rows using J and K
- Change modes by pressing K when at the top of a file

Supported file types:

- .csv  
- .json  
- .parquet  
- .tsv  

## New Features

### Default preview mode is now "summarized". But can be changed by creating an init.lua file in your config/yazi directory. Details below.
- #### Preview mode can be toggled within yazi:
- #### Press "K" at the top of the file to toggle between "standard" and "summarized."
- #### Preview mode is remembered on a per session basis, rather than per file. So if you toggle to standard, it will stay as standard in that session until toggled again.
### Performance improvements through caching:
- #### "Standard" and "summarized" views are cached upon first load, improving scrolling performance.
- #### Note that on entering a directory you haven't entered before (or one containing files that have been changed) cacheing is triggered. Until cache's are generated, summarized mode may take a longer to show as it will be run on the original file, and scrolling other files during this time (especially large ones) can slow things even further as new queries on the file will be competing with cache queries. Instead it is worth waiting until the caches load (displayed in bottom right corner) or switching to standard view during these first few seconds. This will be most apparent on large, non-parquet files.


## Installation

To install, use the command:

ya pack -a wylie102/duckdb

and add to your yazi.toml:

    [plugin]  
    prepend_previewers = [  
      { name = "*.csv", run = "duckdb" },  
      { name = "*.tsv", run = "duckdb" },  
      { name = "*.json", run = "duckdb" },  
      { name = "*.parquet", run = "duckdb" },  
    ]

    prepend_preloaders = [  
      { name = "*.csv", run = "duckdb", multi = false },  
      { name = "*.tsv", run = "duckdb", multi = false },  
      { name = "*.json", run = "duckdb", multi = false },  
      { name = "*.parquet", run = "duckdb", multi = false },  
    ]

If you want to change the default view then create an init.lua file in your yazi folder (where your plugin folder and yazi.toml file live. Add the following:

    -- duckdb plugin
    require("duckdb"):setup({ mode = "standard" })

### Yazi

[Installation installations](https://yazi-rs.github.io/docs/installation)

### duckdb

[Installation instructions](https://duckdb.org/docs/installation/?version=stable&environment=cli&platform=macos&download_method=direct)

## Recommended plugins

Use with a larger preview window or maximize the preview pane plugin:  
<https://github.com/yazi-rs/plugins/tree/main/toggle-pane.yazi>



## Setup and usage changes

Previously, preview mode was selected by setting an environment variable (`DUCKDB_PREVIEW_MODE`).

The new version no longer uses environment variables. Toggle preview modes directly within yazi using the keybinding described above.
The default preview mode value can be set by creating an init.lua file in your config/yazi directory (the one where the plugins folder and yazi.toml file are) and adding this to it:

    -- duckdb plugin
    require("duckdb"):setup({ mode = "standard" })

Scrolling rows within both views (standard and summarized) is handled by pressing J (down) and K (up). Pressing K at the top of a file will change the preview mode.

## Preview

<img width="1710" alt="Screenshot 2025-03-22 at 18 00 06" src="https://github.com/user-attachments/assets/db09fff9-2db1-4273-9ddf-34d0bf087967" />
