-- DuckDB Plugin for Yazi
local M = {}
local st = {}

-- Setup from init.lua: require("duckdb"):setup({ mode = "standard" })
function M:setup(_, opts)
	st.mode = opts and opts.mode or "summarized"
	ya.dbg("DuckDB plugin setup with default mode: " .. st.mode)
end

-- Full summarized SQL
local function generate_sql(job, mode)
	if mode == "standard" then
		return string.format("SELECT * FROM '%s' LIMIT 500", tostring(job.file.url))
	else
		return string.format(
			[[SELECT
				column_name AS column,
				column_type AS type,
				count,
				approx_unique AS unique,
				null_percentage AS null,
				LEFT(min, 10) AS min,
				LEFT(max, 10) AS max,
				CASE
					WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
					WHEN avg IS NULL THEN 'NULL'
					WHEN TRY_CAST(avg AS DOUBLE) IS NULL THEN avg
					WHEN CAST(avg AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(avg AS DOUBLE), 2) AS VARCHAR)
					WHEN CAST(avg AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
					WHEN CAST(avg AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
					WHEN CAST(avg AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(avg AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
					ELSE '∞'
				END AS avg,
				CASE
					WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
					WHEN std IS NULL THEN 'NULL'
					WHEN TRY_CAST(std AS DOUBLE) IS NULL THEN std
					WHEN CAST(std AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(std AS DOUBLE), 2) AS VARCHAR)
					WHEN CAST(std AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
					WHEN CAST(std AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
					WHEN CAST(std AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(std AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
					ELSE '∞'
				END AS std,
				CASE
					WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
					WHEN q25 IS NULL THEN 'NULL'
					WHEN TRY_CAST(q25 AS DOUBLE) IS NULL THEN q25
					WHEN CAST(q25 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q25 AS DOUBLE), 2) AS VARCHAR)
					WHEN CAST(q25 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
					WHEN CAST(q25 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
					WHEN CAST(q25 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
					ELSE '∞'
				END AS q25,
				CASE
					WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
					WHEN q50 IS NULL THEN 'NULL'
					WHEN TRY_CAST(q50 AS DOUBLE) IS NULL THEN q50
					WHEN CAST(q50 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q50 AS DOUBLE), 2) AS VARCHAR)
					WHEN CAST(q50 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
					WHEN CAST(q50 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
					WHEN CAST(q50 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
					ELSE '∞'
				END AS q50,
				CASE
					WHEN column_type IN ('TIMESTAMP', 'DATE') THEN '-'
					WHEN q75 IS NULL THEN 'NULL'
					WHEN TRY_CAST(q75 AS DOUBLE) IS NULL THEN q75
					WHEN CAST(q75 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q75 AS DOUBLE), 2) AS VARCHAR)
					WHEN CAST(q75 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE) / 1000, 1) AS VARCHAR) || 'k'
					WHEN CAST(q75 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE) / 1000000, 2) AS VARCHAR) || 'm'
					WHEN CAST(q75 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE) / 1000000000, 2) AS VARCHAR) || 'b'
					ELSE '∞'
				END AS q75
			FROM (summarize FROM '%s')]],
			tostring(job.file.url)
		)
	end
end

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

local function run_query(job, query, target)
	local args = {}
	if target ~= job.file.url then
		table.insert(args, tostring(target))
	end
	table.insert(args, "-c")
	table.insert(args, query)
	local child = Command("duckdb"):args(args):stdout(Command.PIPED):stderr(Command.PIPED):spawn()
	if not child then
		return nil
	end
	local output, err = child:wait_with_output()
	if err or not output.status.success then
		ya.err("DuckDB error: " .. (err or output.stderr))
		return nil
	end
	return output
end

local function create_cache(job, mode, path)
	if fs.cha(path) then
		return true
	end
	local sql = generate_sql(job, mode)
	local out = run_query(job, string.format("CREATE TABLE My_table AS (%s);", sql), path)
	return out ~= nil
end

-- Preload both cache types
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

-- Peek preview (with toggle-on-scroll-top logic)
function M:peek(job)
	local raw_skip = job.skip or 0
	local skip = math.max(0, raw_skip - 50)

	if raw_skip > 0 and raw_skip < 50 then
		st.mode = (st.mode == "summarized") and "standard" or "summarized"
		ya.dbg("Toggled preview mode to: " .. st.mode)
		ya.manager_emit("peek", { 0, only_if = job.file.url })
		return
	end

	job.skip = skip
	local mode = st.mode or "summarized"
	local cache = get_cache_path(job, mode)
	local target = (cache and fs.cha(cache)) and cache or job.file.url
	local query = generate_sql(job, mode)
	local sql =
		string.format("WITH preview AS (%s) SELECT * FROM preview LIMIT %d OFFSET %d;", query, job.area.h - 7, skip)

	ya.dbg("Generating preview in mode: " .. mode)

	local output = run_query(job, sql, target)
	if not output or output.stdout == "" then
		ya.dbg("Falling back to code preview")
		return require("code"):peek(job)
	end
	ya.preview_widgets(job, { ui.Text.parse(output.stdout):area(job.area) })
end

-- Handle scrolling
function M:seek(job)
	local OFFSET = 50
	local encoded_skip = math.max(0, (cx.active.preview.skip or 0) - OFFSET) + (job.units or 0) + OFFSET
	ya.manager_emit("peek", { encoded_skip, only_if = job.file.url })
end

return M
