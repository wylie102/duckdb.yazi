-- DuckDB Plugin for Yazi
local M = {}

-- TODO: csv ignore errors/alternate quotes
-- TODO: db show tables
-- TODO: xlsx support
-- TODO: ensure errors are transmitted in the preload function
-- TODO: render function fix for peek
-- TODO: versioning for the cache
-- TODO: check if seek/peek still need offset and set a "mode changed" toggle in opts
-- TODO: turn loader bars off.

local set_state = ya.sync(function(state, key, value)
	state.opts = state.opts or {}
	state.opts[key] = value
end)

local get_state = ya.sync(function(state, key)
	state.opts = state.opts or {}
	return state.opts[key]
end)

-- Setup from init.lua: require("duckdb"):setup({ mode = "standard"/"summarized" })
function M:setup(opts)
	opts = opts or {}

	local mode = opts.mode or "summarized"
	local os = ya.target_os()
	local column_width = opts.minmax_column_width or 10

	set_state("mode", mode)
	set_state("os", os)
	set_state("column_width", column_width)

	ya.dbg("Setup - Preview mode initialized to: " .. mode)
	ya.dbg("Setup - OS detected as: " .. os)
	ya.dbg("Setup - Column width set to: " .. column_width)
end

local function generate_preload_query(job, mode)
	if mode == "standard" then
		return string.format("FROM '%s' LIMIT 500", tostring(job.file.url))
	else
		return string.format("SELECT * FROM (SUMMARIZE FROM '%s')", tostring(job.file.url))
	end
end

local function generate_summary_cte(target)
	column_width = get_state("column_width")
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
	local skip = job.skip
	job.skip = 0
	local base = ya.file_cache(job)
	job.skip = skip
	if not base then
		return nil
	end
	return Url(tostring(base) .. "_" .. mode .. ".db")
end

-- Run queries.
local function run_query(job, query, target)
	local args = {}
	if target ~= job.file.url then
		table.insert(args, tostring(target))
	end
	table.insert(args, "-c")
	table.insert(args, query)
	ya.dbg(query)
	local child = Command("duckdb"):args(args):stdout(Command.PIPED):stderr(Command.PIPED):spawn()
	if not child then
		return nil
	end
	local output, err = child:wait_with_output()
	if err or not output.status.success then
		ya.err(err)
		return nil
	end
	return output
end

local function run_query_ascii_preview_mac(job, query, target)
	local db_path = (target ~= job.file.url) and tostring(target) or ""

	local width = math.max((job.area and job.area.w * 10 or 80), 80)
	local height = math.max((job.area and job.area.h or 25), 25)

	local args = { "-q", "/dev/null", "duckdb" }
	if db_path ~= "" then
		table.insert(args, db_path)
	end

	-- Inject duckbox config via separate -c args before the main query
	table.insert(args, "-c")
	table.insert(args, string.format(".maxwidth %d", width))
	table.insert(args, "-c")
	table.insert(args, string.format(".maxrows %d", height))
	table.insert(args, "-c")
	table.insert(args, query)

	ya.dbg("Running script with args: " .. table.concat(args, " "))

	local child = Command("script"):args(args):stdout(Command.PIPED):stderr(Command.PIPED):spawn()

	if not child then
		ya.err("Failed to spawn script")
		return nil
	end

	local output, err = child:wait_with_output()
	if err or not output or not output.status.success then
		ya.err("DuckDB (via script) error: " .. (err or output.stderr or "[unknown error]"))
		return nil
	end

	return output
end

local function create_cache(job, mode, path)
	if fs.cha(path) then
		return true
	end
	local sql = generate_preload_query(job, mode)
	local out = run_query(job, string.format("CREATE TABLE My_table AS %s;", sql), path)
	return out ~= nil
end

local function generate_peek_query(target, job, limit, offset)
	local mode = get_state("mode")
	local is_file = (target == job.file.url)

	if mode == "standard" then
		return string.format(
			"SELECT * FROM %s LIMIT %d OFFSET %d;",
			is_file and ("'" .. target .. "'") or "My_table",
			limit,
			offset
		)
	else
		local summary_source = is_file and string.format("(summarize select * from '%s')", target) or "My_table"

		local summary_cte = generate_summary_cte(summary_source)

		return string.format(
			[[
WITH summary_cte AS (
	%s
)
SELECT * FROM summary_cte LIMIT %d OFFSET %d;
]],
			summary_cte,
			limit,
			offset
		)
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

local function os_run_peek_query(job, target, limit, offset)
	local operating_system = get_state("os")
	local query = generate_peek_query(target, job, limit, offset)
	if operating_system == "macos" then
		return run_query_ascii_preview_mac(job, query, target)
	else
		return run_query(job, query, target)
	end
end

-- Preload summarized and standard preview caches
function M:preload(job)
	-- brief sleep to avoid blocking peek call when entering dir for first time.
	ya.sleep(0.1)
	for _, mode in ipairs({ "standard", "summarized" }) do
		local path = get_cache_path(job, mode)
		if path and not fs.cha(path) then
			local filename = job.file.url:name() or "[unknown]"
			ya.dbg(string.format("Preload - Creating cache for mode: %s, file: %s", mode, filename))
			create_cache(job, mode, path)
			ya.dbg(string.format("Preload - Finished cache for mode: %s, file: %s", mode, filename))
		end
	end
	return true
end

-- Peek with mode toggle if scrolling at top
function M:peek(job)
	local raw_skip = job.skip or 0
	local skip = math.max(0, raw_skip - 50)
	job.skip = skip

	ya.dbg(string.format("Peek - raw_skip: %d, adjusted skip: %d", raw_skip, skip))

	local mode = get_state("mode")
	ya.dbg("Peek - Mode from state: " .. mode)

	local cache = get_cache_path(job, mode)
	local file_url = job.file.url
	local target = (cache and fs.cha(cache)) and cache or file_url

	local limit = job.area.h - 7
	local offset = skip

	ya.dbg(string.format("Peek - target: %s", target))
	local output = os_run_peek_query(job, target, limit, offset)
	if not output or is_duckdb_error(output) then
		if output and output.stdout then
			ya.err("DuckDB returned an error or invalid output:\n" .. output.stdout)
		else
			ya.dbg("Peek - No output from cache.")
		end

		if target ~= file_url then
			ya.dbg("Peek - Retrying directly from file.")
			target = file_url
			output = os_run_peek_query(job, target, limit, offset)
			if not output or is_duckdb_error(output) then
				if output and output.stdout then
					ya.err("Fallback DuckDB output:\n" .. output.stdout)
				end
				ya.dbg("Peek - Fallback also failed. Showing default code preview.")
				return require("code"):peek(job)
			end
		else
			ya.dbg("Peek - No cache and direct file attempt failed. Showing default code preview.")
			return require("code"):peek(job)
		end
	end

	ya.dbg(string.format("Peek - result returned for: %s", file_url:name()))
	ya.preview_widgets(job, { ui.Text.parse(output.stdout:sub(5):gsub("\r", "")):area(job.area) })
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
		ya.dbg("Seek - Toggled mode to: " .. new_mode)

		-- Trigger re-peek
		ya.manager_emit("peek", { OFFSET_BASE, only_if = job.file.url })
	else
		ya.manager_emit("peek", { new_skip + OFFSET_BASE, only_if = job.file.url })
	end
end

return M
