-- DuckDB Plugin for Yazi
local M = {}

local set_mode = ya.sync(function(state, mode)
	state.mode = mode
end)

local get_mode = ya.sync(function(state)
	return state.mode or "summarized"
end)

-- Setup from init.lua: require("duckdb"):setup({ mode = "standard" })
function M:setup(opts)
	local default_mode = opts and opts.mode or "summarized"
	set_mode(default_mode)
	ya.dbg("Setup - Preview mode initialized to: " .. default_mode)
end

-- Full summarized SQL
local function generate_preload_query(job, mode)
	if mode == "standard" then
		return string.format("FROM '%s' LIMIT 500", tostring(job.file.url))
	else
		return string.format("SELECT * FROM (SUMMARIZE FROM '%s')", tostring(job.file.url))
	end
end

local function generate_summary_cte(target)
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
		WHEN TRY_CAST(avg AS DOUBLE) IS NULL THEN avg
		WHEN CAST(avg AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(avg AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(avg AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(avg AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(avg AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS avg,
	CASE
		WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
		WHEN std IS NULL THEN NULL
		WHEN TRY_CAST(std AS DOUBLE) IS NULL THEN std
		WHEN CAST(std AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(std AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(std AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(std AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(std AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS std,
	CASE
		WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
		WHEN q25 IS NULL THEN NULL
		WHEN TRY_CAST(q25 AS DOUBLE) IS NULL THEN q25
		WHEN CAST(q25 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q25 AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(q25 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(q25 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(q25 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS q25,
	CASE
		WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
		WHEN q50 IS NULL THEN NULL
		WHEN TRY_CAST(q50 AS DOUBLE) IS NULL THEN q50
		WHEN CAST(q50 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q50 AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(q50 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(q50 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(q50 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS q50,
	CASE
		WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
		WHEN q75 IS NULL THEN NULL
		WHEN TRY_CAST(q75 AS DOUBLE) IS NULL THEN q75
		WHEN CAST(q75 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q75 AS DOUBLE), 2) AS VARCHAR)
		WHEN CAST(q75 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
		WHEN CAST(q75 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
		WHEN CAST(q75 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
		ELSE '∞'
	END AS q75
FROM %s]],
		10,
		10,
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
	ya.dbg(target)
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
	ya.dbg(sql)
	local out = run_query(job, string.format("CREATE TABLE My_table AS %s;", sql), path)
	return out ~= nil
end

-- TODO: finish peek query generation.
local function generate_peek_query(target, job, limit, offset)
	local mode = get_mode()
	local table_ref = (target == job.url) and ("'" .. tostring(target) .. "'") or "My_table"

	if mode == "standard" then
		return string.format("SELECT * FROM %s LIMIT %d OFFSET %d;", table_ref, limit, offset)
	else
		local summary_cte = generate_summary_cte(table_ref)
		return string.format(
			"WITH summary_cte AS (%s) SELECT * FROM summary_cte LIMIT %d OFFSET %d;",
			summary_cte,
			limit,
			offset
		)
	end
end

-- Preload summarized and standard preview caches
function M:preload(job)
	for _, mode in ipairs({ "standard", "summarized" }) do
		local path = get_cache_path(job, mode)
		if path and not fs.cha(path) then
			ya.dbg("Creating cache for mode: " .. mode)
			create_cache(job, mode, path)
		end
	end
	return true
end

-- Peek with mode toggle if scrolling at top
function M:peek(job)
	local raw_skip = job.skip or 0
	local skip = math.max(0, raw_skip - 50)
	job.skip = skip

	ya.dbg(string.format("Peek - raw_skip: %d | adjusted skip: %d", raw_skip, skip))

	local mode = get_mode()
	ya.dbg("Peek - Mode from state: " .. mode)

	local cache = get_cache_path(job, mode)
	local file_url = job.file.url
	local target = (cache and fs.cha(cache)) and cache or file_url

	local limit = job.area.h - 7
	local offset = skip
	local query = generate_peek_query(target, job, limit, offset)

	local output = run_query_ascii_preview_mac(job, query, target)

	if not output or output.stdout == "" then
		ya.dbg("Peek - No output from cache. Trying file directly.")

		if target ~= file_url then
			target = file_url
			query = generate_peek_query(target, job, limit, offset)
			output = run_query_ascii_preview_mac(job, query, target)

			if not output or output.stdout == "" then
				ya.dbg("Peek - Fallback to file also failed. Showing default code preview.")
				return require("code"):peek(job)
			end
		else
			ya.dbg("Peek - No output and target was already file. Showing default code preview.")
			return require("code"):peek(job)
		end
	end

	ya.dbg("stdout:\n" .. output.stdout)
	ya.preview_widgets(job, { ui.Text.parse(output.stdout):area(job.area) })
end

-- Seek with debug output
function M:seek(job)
	local OFFSET_BASE = 50
	local current_skip = math.max(0, cx.active.preview.skip - OFFSET_BASE)
	local units = job.units or 0
	local new_skip = current_skip + units

	if new_skip < 0 then
		-- Toggle preview mode
		local mode = get_mode()
		local new_mode = (mode == "summarized") and "standard" or "summarized"
		set_mode(new_mode)
		ya.dbg("Seek - Toggled mode to: " .. new_mode)

		-- Trigger re-peek
		ya.manager_emit("peek", { OFFSET_BASE, only_if = job.file.url })
	else
		ya.manager_emit("peek", { new_skip + OFFSET_BASE, only_if = job.file.url })
	end
end

return M
