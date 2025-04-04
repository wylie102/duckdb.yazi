# duckdb.yazi

**Uses  [duckdb](https://github.com/duckdb/duckdb) to quickly preview and summarize data files in [yazi](https://github.com/sxyazi/yazi)!**

<br>

<https://github.com/user-attachments/assets/ff2b11fb-d6fa-4b6a-b1a9-8aceed520189>

<br><br>

## What does it do?

This plugin previews your data files in yazi using DuckDB, with two available view modes:

- Preview csv, tsv, json, or parquet files in the following modes
  - Standard mode (default): Displays the file as a table
  - Summarized mode: Uses DuckDB's summarize function, enhanced with custom formatting for readability
- Preview duckdb databases
  - See the tables and the number of rows, columns, indexes in each. Plus a list of column names in index order.
- Scroll rows using J and K
- Change modes by pressing K when at the top of a file

Supported file types:

- .csv  
- .json  
- .parquet  
- .tsv
- .duckdb
- .db - if file is a duckdb database

<br><br>

## New Features

First of all thank you to [sxyazi](https://github.com/sxyazi) for creating and maintaining yazi, and for helping me fix a particularly annoying bug in this release.

<br>

>**Very latest** - If you want info on the latest (cache related changes) then see [here](https://github.com/wylie102/duckdb.yazi?tab=readme-ov-file#setup-and-usage-changes-from-previous-versions). Otherwise keep reading new features and config options below.
<br>

### Output Syntax Highlighting

- Passes through the colors from the duckdb output as you would see if using directly in the terminal.
- These colors can be configured in your `~/.duckdbrc` file, see the Configuration section for details.
- Feature is only on MacOS at the moment.
  - Planned for linux and Windows but need testers.
  - Please support my [feature request](https://github.com/duckdb/duckdb/discussions/16885) with duckdb which will prevent the need for os specific versions and make implementation easier and more reliable.

<br>

**Syntax highlighting with duckdb's default color scheme.**
<img width="700" alt="Screenshot 2025-04-02 at 14 53 38" src="https://github.com/user-attachments/assets/d2267298-b91b-496c-ae74-1d432b826f6f" />

<br>

**Syntax highlighting with customized color scheme.**
<img width="700" alt="Screenshot 2025-04-02 at 14 44 08" src="https://github.com/user-attachments/assets/965a0a4e-e4ed-4d88-ab95-84cd543f2a58" />

<br>

### Preview DuckDB Databases

- If you open a `.db` or `.duckdb` file directly, the plugin lists all tables in the database.
- Each entry includes:
  - Table name
  - Rows Count
  - Column count
  - Primary key presence
  - Index count
  - All column names (aggregated and in index order)
- Tables are **alphabetically ordered** and paginated for smooth scrolling.
- Reads directly from the db in read only mode for file safety.

<br>
  
<img width="700" alt="Screenshot 2025-04-02 at 14 46 19" src="https://github.com/user-attachments/assets/c640d6f3-d9f6-4d98-acd8-9e4c87c6e728" />

<br>

### More customisation options - row_id (row number) and width of the min/max columns

- Row id - in standard view to help keep track when scrolling, Default is off, but can be turned on in `init.lua` options.
- Width of min and max columns. Default is now 21 twice as wide as previously. Is now customisable in the `init.lua`, the unit is the number of characters shown.

<br>

<img width="700" alt="Screenshot 2025-04-02 at 14 49 26" src="https://github.com/user-attachments/assets/6c8fb1ae-3de8-41ce-9c90-0279dc3b5e61" />

<br><br>

## Aditions in Previous Update

### Default preview mode is now toggleable

- Preview mode can be toggled within yazi
- Press "K" at the top of the file to toggle between "standard" and "summarized."
- Preview mode is remembered on a per session basis, rather than per file.
- Is customisable in the `init.lua` see Configuration section.

### Performance improvements through caching

- "Standard" and "summarized" views are cached upon first load, improving scrolling performance

- Note that on entering a directory you haven't entered before (or one containing files that have been changed) cacheing is triggered. Until cache's are generated, summarized mode may take a longer to show as it will be run on the original file, and scrolling other files during this time (especially large ones) can slow things even further as new queries on the file will be competing with cache queries. Instead it is worth waiting until the caches load (displayed in bottom right corner) or switching to standard view during these first few seconds. This will be most apparent on large, non-parquet files

<br><br>

## Installation

First you will need Yazi and DuckDB installed.

- [Yazi Installation instructions](https://yazi-rs.github.io/docs/installation)

- [DuckDB Installation instructions](https://duckdb.org/docs/installation/?version=stable&environment=cli&platform=macos&download_method=direct)

Once these are installed you can use the yazi plugin manager to install the plugin.

Use the command:

```
ya pack -a wylie102/duckdb
```

in your terminal

Then navigate to your [yazi.toml](https://yazi-rs.github.io/docs/configuration/yazi#manager.ratio) file this should be the `yazi` folder in your `config` directory

and add:

```toml
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
      { name = "*.db", run = "duckdb" },
      { name = "*.duckdb", run = "duckdb" },
    ]
```

### Aditional setup and recommended plugins for more preview space

Use with a larger preview window - add to your `yazi.toml`

```toml
[manager]
ratio = [1, 2, 5]
```

For reference the default ratio is 1, 4, 3

Use:

[maximize the preview pane plugin](https://github.com/yazi-rs/plugins/tree/main/toggle-pane.yazi)

<br><br>

## Configuration/Customisation

Configuration of yazi.duckdb is done via the `init.lua` file in `config/yazi` (where your plugin folder and yazi.toml file live).
If you don't have one you can just create one.
Add the following:

```lua
    -- DuckDB plugin configuration
require("duckdb"):setup({
  mode = "standard",            -- Default: "summarized"
  row_id = true,                -- Default: false
  minmax_column_width = 30      -- Default: 21
})
```

Configuration of DuckDB can be done in the `~/.duckdbrc` file.
This should be placed in your home directory ([duckdb docs](https://duckdb.org/docs/stable/operations_manual/footprint_of_duckdb/files_created_by_duckdb)).

You can customise the colors of the preview using the following options

```
.highlight_colors layout gray 
.highlight_colors column_name magenta bold
.highlight_colors column_type gray
.highlight_colors string_value cyan
.highlight_colors numeric_value green
.highlight_colors temporal_value blue
.highlight_colors footer gray
```

The above configuration is what is used in the video at the top of the readme and in the screenshots of the color highlithing section.
Although the actual colours will depend on your terminal/yazi color scheme.
These should be placed in your `~./duckdbrc` file as is.
No header is needed, they are simply commands run on the startup of any duckdb instance (when using the CLI).
These will change the color of the output in both duckdb.yazi and when using it in the CLI.

Color options are:
red|green|yellow|blue|magenta|cyan|white

You can also specify bold, underline or bold_underline after the colors
e.g. `.highlight_colors column_type red bold_underline`

If the file is empty or doesn't exist then the default duckdb color scheme will be used
This uses gray for borders and NULLs and looks like this

<img width="700" alt="Screenshot 2025-04-02 at 14 53 38" src="https://github.com/user-attachments/assets/d2267298-b91b-496c-ae74-1d432b826f6f" />

You can also turn the highlighting off by adding `.highlight_results off`
In which case it will look like below. (Note this is also how it will look on non mac os systems currently).

<img width="700" alt="Screenshot 2025-03-22 at 18 00 06" src="https://github.com/user-attachments/assets/db09fff9-2db1-4273-9ddf-34d0bf087967" />

More information [here](https://duckdb.org/docs/stable/clients/cli/dot_commands#configuring-the-result-syntax-highlighter)

<br><br>

## Setup and usage changes from previous versions

### A Note on the Latest update
The caches are now stored as parquet files, previously they were mini duckdb databases because the duckdb documentation suggested better performance with similar file size to parquet. However, there seemt to be a minimum file size of ~500kb regardless of the amount of data stored. While that isn't huge, it could add up.

So I did some tests and using parquet files we get file sizes of ~5kb for the summarized view and usually 70-130kb for standard view. So about 1/10th the size for both views. Load speeds were similar (or slightly faster) using parquet, for both caching and viewing operations.

So we've moved to parquet. I have cache versioning implemented, so yazi will automatically switch to using the latest version. But won't delete the old cache files. These are temp files and will usually be deleted on reboot, but if you like you can clear the whole cache using `yazi --clear-cache`.

### Original version
Previously, preview mode was selected by setting an environment variable (`DUCKDB_PREVIEW_MODE`).

The new version no longer uses environment variables. Toggle preview modes directly within yazi using the keybinding described in the New Features section.
The default preview mode value can be set by creating an init.lua file in your config/yazi directory. See configuration above.
