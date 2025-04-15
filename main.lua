--- @since 25.4.8
-- DuckDB Plugin for Yazi
local M = {}

local update_state = ya.sync(function(state, action, category, key, value)
	-- Ensure the subtable for the category exists.
	state[category] = state[category] or {}

	if action == "set" then
		state[category][key] = value
	elseif action == "get" then
		return state[category][key]
	elseif action == "check" then
		return state[category][key] ~= nil
	else
		ya.err("Unknown action: " .. tostring(action))
	end
end)

local function set_opts(key, value)
	update_state("set", "opts", key, value)
end

local function get_opts(key)
	return update_state("get", "opts", key)
end

local function add_to_list(category, cache_str)
	update_state("set", category, cache_str, true)
end

local function remove_from_list(category, cache_str)
	update_state("set", category, cache_str, nil)
end

local function is_on_list(category, cache_str)
	return update_state("check", category, cache_str)
end

local function clear_list(category)
	set_opts(category, {}) -- replaces the whole list with an empty table
end

function M:entry(job)
	local scroll_delta = tonumber(job.args and job.args[1])

	if not scroll_delta then
		ya.err("DuckDB column scroll entry: Invalid or missing scroll delta; exiting.")
		return
	end

	local scrolled_columns = get_opts("scrolled_columns") or 0
	scrolled_columns = math.max(0, scrolled_columns + scroll_delta)
	set_opts("scrolled_columns", scrolled_columns)

	ya.manager_emit("seek", { "lateral scroll" })
end

-- Setup from init.lua: require("duckdb"):setup({ mode = "standard"/"summarized" })
function M:setup(opts)
	opts = opts or {}

	local mode = opts.mode or "summarized"
	local operating_system = ya.target_os()
	local column_width = opts.minmax_column_width or 21
	local row_id = opts.row_id
	if row_id == nil then
		row_id = false
	end
	local column_fit_factor = opts.column_fit_factor or 10

	set_opts("mode", mode)
	set_opts("mode_changed", false)
	set_opts("re_peek", false)
	set_opts("os", operating_system)
	set_opts("column_width", column_width)
	set_opts("row_id", row_id)
	set_opts("scrolled_columns", 0)
	set_opts("column_fit_factor", column_fit_factor)
end

local function generate_preload_query(job, mode)
	if mode == "standard" then
		return string.format("FROM '%s' LIMIT 500", tostring(job.file.url))
	else
		return string.format(
			"SELECT * EXCLUDE(null_percentage), CAST(null_percentage AS DOUBLE) AS null_percentage FROM (SUMMARIZE FROM '%s')",
			tostring(job.file.url)
		)
	end
end

local function generate_summary_cte(target)
	local column_width = get_opts("column_width")
	return string.format(
		[[
SELECT
	column_name AS column,
	column_type AS type,
	count,
	approx_unique AS unique,
	null_percentage AS "null%%",
	LEFT(min, %d) AS min,
	LEFT(max, %d) AS max,
	CASE
		WHEN avg IS NULL THEN NULL
		WHEN TRY_CAST(avg AS DOUBLE) IS NULL THEN CAST(avg AS VARCHAR)
		WHEN CAST(avg AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(avg AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(avg AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(avg AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(avg AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS avg,
	CASE
		WHEN std IS NULL THEN NULL
		WHEN TRY_CAST(std AS DOUBLE) IS NULL THEN CAST(std AS VARCHAR)
		WHEN CAST(std AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(std AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(std AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(std AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(std AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS std,
	CASE
		WHEN q25 IS NULL THEN NULL
    WHEN column_type = 'TIMESTAMP' THEN coalesce(strftime(try_strptime(q25::VARCHAR, '%%c.%%f'), '%%c'), q25::VARCHAR)
		WHEN TRY_CAST(q25 AS DOUBLE) IS NULL THEN CAST(q25 AS VARCHAR)
		WHEN CAST(q25 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q25 AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(q25 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(q25 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(q25 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS q25,
	CASE
		WHEN q50 IS NULL THEN NULL
    WHEN column_type = 'TIMESTAMP' THEN coalesce(strftime(try_strptime(q50::VARCHAR, '%%c.%%f'), '%%c'), q50::VARCHAR)
		WHEN TRY_CAST(q50 AS DOUBLE) IS NULL THEN CAST(q50 AS VARCHAR)
		WHEN CAST(q50 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q50 AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(q50 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(q50 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(q50 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS q50,
	CASE
		WHEN q75 IS NULL THEN NULL
    WHEN column_type = 'TIMESTAMP' THEN coalesce(strftime(try_strptime(q75::VARCHAR, '%%c.%%f'), '%%c'), q75::VARCHAR)
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
	local cache_version = 3
	local skip = job.skip
	job.skip = 1000000 + cache_version
	local base = ya.file_cache(job)
	job.skip = skip

	if not base then
		return nil, nil
	end

	local base_str = tostring(base) .. "_" .. mode .. ".parquet"
	local path_url = Url(base_str)
	local path_str = tostring(path_url.name)
	return path_str, path_url
end

local extension_map = {
	csv = "csv",
	tsv = "csv",
	txt = "text",
	json = "json",
	parquet = "parquet",
	xlsx = "excel",
	duckdb = "duckdb",
	db = "duckdb",
}

local function get_extension(filename)
	-- Match the last "dot + word characters" at the end of the string
	return filename:match("^.+%.([a-zA-Z0-9]+)$")
end

local function check_file_type(path)
	local name = path.name or ""
	local ext = get_extension(name)
	if ext then
		local filetype = extension_map[ext:lower()]
		if filetype then
			return filetype
		end
	end
	ya.err("File is not a supported file type")
end

-- Run queries.
local function run_query(job, query, target, file_type)
	local width = math.max((job.area and job.area.w * 3 or 80), 80)
	local height = math.max((job.area and job.area.h or 25), 25)

	local args = {}

	if file_type == "duckdb" then
		table.insert(args, "-readonly")
		table.insert(args, tostring(target))
	end

	-- Duckbox config
	table.insert(args, "-c")
	table.insert(args, ".mode duckbox")
	table.insert(args, "-c")
	table.insert(args, ".timer off")
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

local function generate_db_query(limit, offset)
	local scroll = get_opts("scrolled_columns") or 0

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
	local scroll = get_opts("scrolled_columns") or 0
	local args = {}
	local actual_width = math.max((job.area and job.area.w or 80), 80)
	local column_fit_factor = get_opts("column_fit_factor") or 7
	local fetched_columns = math.floor(actual_width / column_fit_factor) + scroll
	local row_id_mode = get_opts("row_id")

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
	local scroll = get_opts("scrolled_columns") or 0

	-- These are the scrollable fields, in display order
	local fields = {
		'"type"',
		'"count"',
		'"unique"',
		'"null%"',
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

local function generate_peek_query(target, job, limit, offset, file_type, cache_str)
	local mode = get_opts("mode")
	local is_original_file = (target == job.file.url)

	-- If the file itself is a DuckDB database, list tables/columns
	if is_original_file and file_type == "duckdb" then
		return generate_db_query(limit, offset)
	end

	local source = "'" .. tostring(target) .. "'"

	if mode == "standard" then
		return generate_standard_query(source, job, limit, offset)
	end
	local placeholder = "⏱"
	if is_on_list("bad_cache", cache_str) then
		placeholder = "∅"
	end

	if file_type ~= "parquet" then
		local summary_source = is_original_file
				and string.format(
					[[(select
          column_name,
          column_type,
          '   %s ' as count,
          '   %s ' as "approx_unique",
          '   %s ' as "null_percentage",
          '   %s ' as min,
          '   %s ' as max,
          '   %s ' as avg,
          '   %s ' as std,
          '   %s ' as q25,
          '   %s ' as q50,
          '   %s ' as q75
          from (describe select * from %s))]],
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					source
				)
			or source
		return generate_summarized_query(summary_source, limit, offset)
	else
		local summary_source = is_original_file
				and string.format(
					[[
        (select
        d.column_name,
        d.column_type,
        sum(m.num_values) as count,
        '   %s ' as "approx_unique",
        '   %s ' as "null_percentage",
        case when min(m.stats_min) is null then '%s' else min(m.stats_min) end as min,
        case when min(m.stats_max) is null then '%s' else max(m.stats_max) end as max,
        '   %s ' as "avg",
        '   %s ' as "std",
        '   %s ' as q25,
        '   %s ' as q50,
        '   %s ' as q75
        from (describe select * from %s) d 
        left join parquet_metadata(%s) m 
        on d.column_name = m.path_in_schema
        group by all
        order by min(column_id))
        ]],
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					placeholder,
					source,
					source
				)
			or source
		return generate_summarized_query(summary_source, limit, offset)
	end
end

local function output_is_valid(output, mode, job)
	if output then
		if output.stderr and output.stderr ~= "" then
			ya.err("DuckDB returned an error or:\n" .. output.stderr)
			return false
		elseif not output.stdout or output.stdout == "" then
			ya.err(string.format("Peek - No stdout/stderr from %s cache for %s", mode, job.file.url))
			return false
		else
			return true
		end
	else
		ya.err("Duckdb failed to return output")
		return false
	end
end

local function remove_file(cache_url)
	if fs.cha(cache_url) then
		local ok, err = fs.remove("file", cache_url)
		if not ok then
			ya.err(
				string.format("[duckdb] failed to remove partial cache at %s: %s", tostring(cache_url), tostring(err))
			)
		end
	end
end

local function create_cache(job, mode, cache_url, file_type)
	local base_query = generate_preload_query(job, mode)
	local output = run_query(
		job,
		string.format("COPY (%s) TO '%s' (FORMAT 'parquet');", base_query, tostring(cache_url)),
		job.file.url,
		file_type
	)

	if not output or (output.stderr and output.stderr ~= "") then
		-- Log error message
		if not output then
			ya.err(
				string.format(
					"[duckdb] no output returned while creating %s cache for %s",
					mode,
					tostring(job.file.url)
				)
			)
		else
			ya.err(
				string.format(
					"[duckdb] error creating %s cache for %s: %s",
					mode,
					tostring(job.file.url),
					output.stderr
				)
			)
		end
		remove_file(cache_url)
		return false
	end
	return true
end

-- Preload summarized and standard preview caches
function M:preload(job)
	local file_type = check_file_type(job.file.url)
	if file_type == "duckdb" then
		return true
	end

	local all_done = true

	for _, mode in ipairs({ "standard", "summarized" }) do
		local cache_str, cache_url = get_cache_path(job, mode)

		if cache_url and not fs.cha(cache_url) and not is_on_list("bad_cache", cache_str) then
			-- Cache doesn't exist, so create it
			add_to_list("preloading", cache_str)

			local success = create_cache(job, mode, cache_url, file_type)

			remove_from_list("preloading", cache_str)
			add_to_list("completed", cache_str)
			if not success then
				add_to_list("bad_cache", cache_str)
				all_done = false
			end
		end
	end

	return all_done
end

-- Peek with mode toggle if scrolling at top
function M:peek(job)
	local file_url = job.file.url

	if not get_opts("re_peek") then
		local raw_skip = job.skip or 0
		if raw_skip == 0 then
			set_opts("scrolled_columns", 0)
		end
		if get_opts("mode_changed") then
			set_opts("scrolled_columns", 0)
			set_opts("mode_changed", false)
		end
		local skip = math.max(0, raw_skip - 50)
		job.skip = skip
	end
	set_opts("re_peek", false)

	local mode = get_opts("mode")
	local file_type = check_file_type(file_url)

	local cache_str, cache_url = get_cache_path(job, mode)

	local use_cache = cache_url
		and fs.cha(cache_url)
		and not is_on_list("preloading", cache_str)
		and not is_on_list("bad_cache", cache_str)
	local target = use_cache and cache_url or file_url

	local limit = job.area.h - 7
	local offset = job.skip

	local query = generate_peek_query(target, job, limit, offset, file_type, cache_str)
	local output = run_query(job, query, target, file_type)
	if not output_is_valid(output, mode, job) then
		if target ~= file_url then
			add_to_list("bad_cache", cache_str)
			remove_file(cache_url)
			return require("duckdb"):peek(job)
		elseif is_on_list("bad_cache", cache_str) then
			return require("code"):peek(job)
		end
	end

	if target == file_url and not use_cache and mode == "summarized" then
		ya.preview_widgets(job, {
			ui.Text.parse(output.stdout:gsub("\r", "")):area(job.area),
		})
		while not is_on_list("completed", cache_str) do
			ya.sleep(0.2)
		end
		clear_list("completed")
		set_opts("re_peek", true)
		return require("duckdb"):peek(job)
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
		local mode = get_opts("mode")
		local new_mode = (mode == "summarized") and "standard" or "summarized"
		set_opts("mode", new_mode)
		set_opts("mode_changed", true)
		-- Trigger re-peek
		ya.manager_emit("peek", { OFFSET_BASE, only_if = job.file.url })
	else
		ya.manager_emit("peek", { new_skip + OFFSET_BASE, only_if = job.file.url })
	end
end

return M
