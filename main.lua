--- @since 25.4.8
-- DuckDB Plugin for Yazi
local M = {}

local set_state = ya.sync(function(state, key, value)
	state.opts = state.opts or {}
	state.opts[key] = value
end)

local get_state = ya.sync(function(state, key)
	state.opts = state.opts or {}
	return state.opts[key]
end)

function M:entry(job)
	local scroll_delta = tonumber(job.args and job.args[1])

	if not scroll_delta then
		ya.err("DuckDB column scroll entry: Invalid or missing scroll delta; exiting.")
		return
	end

	local scrolled_columns = get_state("scrolled_columns") or 0
	scrolled_columns = math.max(0, scrolled_columns + scroll_delta)
	set_state("scrolled_columns", scrolled_columns)

	ya.manager_emit("seek", { "lateral scroll" })
end

-- Setup from init.lua: require("duckdb"):setup({ mode = "standard"/"summarized" })
function M:setup(opts)
	opts = opts or {}

	local mode = opts.mode or "summarized"
	local os = ya.target_os()
	local column_width = opts.minmax_column_width or 21
	local row_id = opts.row_id
	if row_id == nil then
		row_id = false
	end
	local column_fit_factor = opts.column_fit_factor or 10

	set_state("mode", mode)
	set_state("mode_changed", false)
	set_state("os", os)
	set_state("column_width", column_width)
	set_state("row_id", row_id)
	set_state("scrolled_columns", 0)
	set_state("column_fit_factor", column_fit_factor)
end

local function generate_preload_query(job, mode)
	if mode == "standard" then
		return string.format("FROM '%s' LIMIT 500", tostring(job.file.url))
	else
		return string.format("SELECT * FROM (SUMMARIZE FROM '%s')", tostring(job.file.url))
	end
end

local function generate_summary_cte(target)
	local column_width = get_state("column_width")
	return string.format(
		[[
SELECT
	column_name AS column,
	column_type AS type,
	count,
	approx_unique AS unique,
	null_percentage AS null,
	LEFT(min, %d) AS min,
	LEFT(max, %d) AS max,
	CASE
		WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
		WHEN avg IS NULL THEN NULL
		WHEN TRY_CAST(avg AS DOUBLE) IS NULL THEN CAST(avg AS VARCHAR)
		WHEN CAST(avg AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(avg AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(avg AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(avg AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(avg AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS avg,
	CASE
		WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
		WHEN std IS NULL THEN NULL
		WHEN TRY_CAST(std AS DOUBLE) IS NULL THEN CAST(std AS VARCHAR)
		WHEN CAST(std AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(std AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(std AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(std AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(std AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS std,
	CASE
		WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
		WHEN q25 IS NULL THEN NULL
		WHEN TRY_CAST(q25 AS DOUBLE) IS NULL THEN CAST(q25 AS VARCHAR)
		WHEN CAST(q25 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q25 AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(q25 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(q25 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(q25 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS q25,
	CASE
		WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
		WHEN q50 IS NULL THEN NULL
		WHEN TRY_CAST(q50 AS DOUBLE) IS NULL THEN CAST(q50 AS VARCHAR)
		WHEN CAST(q50 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q50 AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(q50 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(q50 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(q50 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS q50,
	CASE
		WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
		WHEN q75 IS NULL THEN NULL
		WHEN TRY_CAST(q75 AS DOUBLE) IS NULL THEN CAST(q75 AS VARCHAR)
		WHEN CAST(q75 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q75 AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(q75 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(q75 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(q75 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS q75
FROM %s
		]],
		column_width,
		column_width,
		target
	)
end

-- Get preview cache path
local function get_cache_path(job, mode)
	local cache_version = 2
	local skip = job.skip
	job.skip = 1000000 + cache_version
	local base = ya.file_cache(job)
	job.skip = skip
	if not base then
		return nil
	end
	return Url(tostring(base) .. "_" .. mode .. ".parquet")
end

local function is_duckdb_database(path)
	local name = path.name or ""
	return name:match("%.duckdb$") or name:match("%.db$")
end

-- Run queries.
local function run_query(job, query, target)
	local width = math.max((job.area and job.area.w * 3 or 80), 80)
	local height = math.max((job.area and job.area.h or 25), 25)

	local args = {}

	if is_duckdb_database(target) then
		table.insert(args, "-readonly")
		table.insert(args, tostring(target))
	end

	-- Duckbox config
	table.insert(args, "-c")
	table.insert(args, "SET enable_progress_bar = false;")
	table.insert(args, "-c")
	table.insert(args, string.format(".maxwidth %d", width))
	table.insert(args, "-c")
	table.insert(args, string.format(".maxrows %d", height))
	table.insert(args, "-c")
	table.insert(args, ".highlight_results on")

	-- Add query (string or list of -c args)
	if type(query) == "table" then
		for _, item in ipairs(query) do
			table.insert(args, item)
		end
	else
		table.insert(args, "-c")
		table.insert(args, query)
	end

	local child = Command("duckdb"):args(args):stdout(Command.PIPED):stderr(Command.PIPED):spawn()
	if not child then
		ya.err("Failed to spawn DuckDB")
		return nil
	end

	local output, err = child:wait_with_output()
	if err or not output.status.success then
		ya.err("DuckDB error: " .. (err or output.stderr or "[unknown error]"))
		return nil
	end

	return output
end

local function create_cache(job, mode, path)
	if fs.cha(path) then
		return true
	end

	local sql = generate_preload_query(job, mode)
	local out =
		run_query(job, string.format("COPY (%s) TO '%s' (FORMAT 'parquet');", sql, tostring(path)), job.file.url)
	return out ~= nil
end

local function generate_db_query(limit, offset)
	local scroll = get_state("scrolled_columns") or 0

	local metadata_fields = { "rows", "columns", "has_pk", "indexes" }
	local visible_column_count = 10
	local max_scroll_metadata = #metadata_fields
	local metadata_projection = { "table_name" }

	if scroll < max_scroll_metadata then
		for i = scroll + 1, #metadata_fields do
			table.insert(metadata_projection, metadata_fields[i])
		end
		table.insert(metadata_projection, "column_names") -- always show

		local projection = table.concat(metadata_projection, ", ")
		return string.format(
			[[
WITH table_info AS (
  SELECT
    DISTINCT t.table_name,
    t.estimated_size AS rows,
    t.column_count AS columns,
    t.has_primary_key AS has_pk,
    t.index_count AS indexes,
    STRING_AGG(c.column_name, ', ' ORDER BY c.column_index) OVER (PARTITION BY t.table_name) AS column_names
  FROM duckdb_tables() t
  LEFT JOIN duckdb_columns() c ON t.table_name = c.table_name
)
SELECT %s FROM table_info
ORDER BY table_name
LIMIT %d OFFSET %d;
]],
			projection,
			limit,
			offset
		)
	else
		local column_scroll = scroll - max_scroll_metadata
		local start_pos = column_scroll + 1
		local end_pos = column_scroll + visible_column_count

		return string.format(
			[[
WITH raw AS (
  SELECT
    t.table_name,
    c.column_name,
    row_number() OVER (PARTITION BY t.table_name ORDER BY c.column_index) AS col_pos
  FROM duckdb_tables() t
  LEFT JOIN duckdb_columns() c ON t.table_name = c.table_name
),
scrolling AS (
  SELECT
    table_name,
    column_name,
    col_pos
  FROM raw
  WHERE col_pos >= %d AND col_pos < %d
),
aggregated AS (
  SELECT
    table_name,
    STRING_AGG(column_name, ', ' ORDER BY col_pos) AS column_names
  FROM scrolling
  GROUP BY table_name
)
SELECT table_name, column_names FROM aggregated
ORDER BY table_name
LIMIT %d OFFSET %d;
]],
			start_pos,
			end_pos,
			limit,
			offset
		)
	end
end

local function generate_standard_query(target, job, limit, offset)
	local scroll = get_state("scrolled_columns") or 0
	local args = {}
	local actual_width = math.max((job.area and job.area.w or 80), 80)
	local column_fit_factor = get_state("column_fit_factor") or 7
	local fetched_columns = math.floor(actual_width / column_fit_factor) + scroll
	local row_id_mode = get_state("row_id")

	-- Determine if row_id should be prepended
	local row_id_prefix = ""
	local row_id_enabled = (row_id_mode == true) or (row_id_mode == "dynamic" and scroll > 0)
	if row_id_enabled then
		row_id_prefix = "row_number() over () as row, "
	end

	local excluded_column_cte = string.format(
		[[
set variable included_columns = (
	with column_list as (
		select column_name, row_number() over () as row
		from (describe select * from %s)
	)
	select list(column_name)
	from column_list
	where row > %d and row <= (%d)
);
]],
		target,
		scroll,
		fetched_columns
	)

	local filtered_select = string.format(
		"select %scolumns(c -> list_contains(getvariable('included_columns'), c)) from %s limit %d offset %d;",
		row_id_prefix,
		target,
		limit,
		offset
	)

	table.insert(args, "-c")
	table.insert(args, excluded_column_cte)
	table.insert(args, "-c")
	table.insert(args, filtered_select)
	return args
end

local function generate_summarized_query(source, limit, offset)
	local scroll = get_state("scrolled_columns") or 0

	-- These are the scrollable fields, in display order
	local fields = {
		'"type"',
		'"count"',
		'"unique"',
		'"null"',
		'"min"',
		'"max"',
		'"avg"',
		'"std"',
		'"q25"',
		'"q50"',
		'"q75"',
	}

	-- Always include the column name
	local selected_fields = { '"column"' }

	-- Add scrollable fields from scroll onwards
	for i = scroll + 1, #fields do
		table.insert(selected_fields, fields[i])
	end

	local summary_cte = generate_summary_cte(source)
	local projection = table.concat(selected_fields, ", ")

	return string.format(
		[[
WITH summary_cte AS (
	%s
)
SELECT %s FROM summary_cte LIMIT %d OFFSET %d;
]],
		summary_cte,
		projection,
		limit,
		offset
	)
end

local function generate_peek_query(target, job, limit, offset)
	local mode = get_state("mode")
	local is_original_file = (target == job.file.url)

	-- If the file itself is a DuckDB database, list tables/columns
	if is_original_file and is_duckdb_database(job.file.url) then
		return generate_db_query(limit, offset)
	end

	local source = "'" .. tostring(target) .. "'"

	if mode == "standard" then
		return generate_standard_query(source, job, limit, offset)
	else
		local summary_source = is_original_file and string.format("(summarize select * from %s)", source) or source
		return generate_summarized_query(summary_source, limit, offset)
	end
end

local function is_duckdb_error(output)
	if not output or not output.stdout then
		return true
	end

	local head = output.stdout:sub(1, 256)

	if head:match("\27%[1m\27%[31m[%w%s]+Error:") then
		return true
	end

	return false
end

-- Preload summarized and standard preview caches
function M:preload(job)
	for _, mode in ipairs({ "standard", "summarized" }) do
		local path = get_cache_path(job, mode)
		if path and not fs.cha(path) then
			-- brief sleep to avoid blocking peek call when entering dir for first time.
			ya.sleep(0.1)
			create_cache(job, mode, path)
		end
	end
	return true
end

-- Peek with mode toggle if scrolling at top
function M:peek(job)
	local raw_skip = job.skip or 0
	if raw_skip == 0 then
		set_state("scrolled_columns", 0)
	end
	if get_state("mode_changed") then
		set_state("scrolled_columns", 0)
		set_state("mode_changed", false)
	end
	local skip = math.max(0, raw_skip - 50)
	job.skip = skip

	local mode = get_state("mode")

	local cache = get_cache_path(job, mode)
	local file_url = job.file.url
	local target = (cache and fs.cha(cache)) and cache or file_url

	local limit = job.area.h - 7
	local offset = skip

	local query = generate_peek_query(target, job, limit, offset)
	local output = run_query(job, query, target)
	if not output or is_duckdb_error(output) then
		if output and output.stdout then
			ya.err("DuckDB returned an error or invalid output:\n" .. output.stdout)
		else
			ya.err("Peek - No output from cache.")
		end

		if target ~= file_url then
			target = file_url
			query = generate_peek_query(target, job, limit, offset)
			output = run_query(job, query, target)
			if not output or is_duckdb_error(output) then
				if output and output.stdout then
					ya.err("Fallback DuckDB output:\n" .. output.stdout)
				end
				return require("code"):peek(job)
			end
		else
			return require("code"):peek(job)
		end
	end

	ya.preview_widgets(job, {
		ui.Text.parse(output.stdout:gsub("\r", "")):area(job.area),
	})
end

-- Seek, also triggers mode change if skip negative.
function M:seek(job)
	local OFFSET_BASE = 50
	local current_skip = math.max(0, cx.active.preview.skip - OFFSET_BASE)
	local units = job.units or 0
	local new_skip = current_skip + units

	if new_skip < 0 then
		-- Toggle preview mode
		local mode = get_state("mode")
		local new_mode = (mode == "summarized") and "standard" or "summarized"
		set_state("mode", new_mode)
		set_state("mode_changed", true)
		-- Trigger re-peek
		ya.manager_emit("peek", { OFFSET_BASE, only_if = job.file.url })
	else
		ya.manager_emit("peek", { new_skip + OFFSET_BASE, only_if = job.file.url })
	end
end

return M
