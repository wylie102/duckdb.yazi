-- This function generates the SQL query based on the preview mode.
local function generate_sql(job, mode)
	if mode == "standard" then
		return string.format("SELECT * FROM '%s' LIMIT 500", tostring(job.file.url))
	else
		return string.format(
			[[
    SELECT
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
    FROM (summarize FROM '%s')
      ]],
			tostring(job.file.url)
		)
	end
end

local function get_cache_path(job, type)
	local skip = job.skip
	job.skip = 0

	local base = ya.file_cache(job)

	job.skip = skip
	if not base then
		return nil
	end

	local suffix = ({
		standard = "_standard.db",
		summarized = "_summarized.db",
		mode = "_mode.db",
	})[type or "standard"]

	return Url(tostring(base) .. suffix)
end

local function run_query(job, query, target)
	local args = {}

	-- Only include the target if it's a cache.
	if target ~= job.file.url then
		table.insert(args, tostring(target))
	end

	table.insert(args, "-c")
	table.insert(args, query)

	ya.dbg("Running DuckDB query on: " .. tostring(job.file.url:name()))

	-- run query
	local child = Command("duckdb"):args(args):stdout(Command.PIPED):stderr(Command.PIPED):spawn()

	if not child then
		ya.dbg("Failed to spawn DuckDB command, falling back")
		return nil
	end

	local output, err = child:wait_with_output()
	if err then
		ya.dbg("DuckDB command error: " .. tostring(err))
		return nil
	end

	if not output.status.success then
		ya.err("DuckDB exited with error: " .. output.stderr)
		return nil
	end

	return output
end

local function create_cache(job, mode, path)
	local filename = job.file.url:name() or "unknown"

	if fs.cha(path) then
		return true
	end

	ya.dbg("Preload - Creating " .. mode .. " cache for file: " .. tostring(filename))
	local start_time = ya.time()

	local sql
	if mode == "mode" then
		sql = "CREATE TABLE My_table AS SELECT 'standard' AS Preview_mode;"
	else
		sql = string.format("CREATE TABLE My_table AS (%s);", generate_sql(job, mode))
	end

	local out = run_query(job, sql, path, mode == "mode" and "mode" or nil)
	local elapsed = ya.time() - start_time

	if not out then
		ya.err("Preload - Failed to generate " .. mode .. " cache for file: " .. tostring(filename) .. ".")
		return false
	else
		ya.dbg(
			string.format(
				"Preload - %s cache created for file: %s (%.3f sec)",
				mode:sub(1, 1):upper() .. mode:sub(2),
				tostring(filename),
				elapsed
			)
		)
		return true
	end
end

local function get_preview_mode(job)
	local mode = "standard"
	local mode_cache = get_cache_path(job, "mode")

	if not mode_cache then
		return mode
	end

	if not fs.cha(mode_cache) then
		ya.dbg("Mode cache not found, creating one with default 'standard'.")
		create_cache(job, "mode", mode_cache)
	end

	local result = run_query(job, "SELECT Preview_mode FROM My_table LIMIT 1;", mode_cache, "mode")
	if result and result.stdout and result.stdout ~= "" then
		local value = result.stdout:lower()
		if value:match("summarized") then
			mode = "summarized"
		elseif value:match("standard") then
			mode = "standard"
		end
	else
		ya.dbg("Mode cache exists but couldn't read Preview_mode.")
	end

	return mode
end

local function generate_query(target, job, limit, offset)
	local mode = get_preview_mode(job)

	if target == job.file.url then
		if mode == "standard" then
			return string.format("SELECT * FROM '%s' LIMIT %d OFFSET %d;", tostring(target), limit, offset)
		else
			local query = generate_sql(job, mode)
			return string.format("WITH query AS (%s) SELECT * FROM query LIMIT %d OFFSET %d;", query, limit, offset)
		end
	else
		return string.format("SELECT * FROM My_table LIMIT %d OFFSET %d;", limit, offset)
	end
end

local function set_preview_mode(job, mode)
	local mode_cache = get_cache_path(job, "mode")
	if not mode_cache then
		ya.err("SetPreviewMode - Could not compute mode cache path.")
		return false
	end

	-- Wipe and insert new mode
	run_query(job, "DELETE FROM My_table;", mode_cache, "mode")
	local sql = string.format("INSERT INTO My_table VALUES ('%s');", mode)
	local result = run_query(job, sql, mode_cache, "mode")

	if result then
		ya.dbg("SetPreviewMode - Mode set to: " .. mode)
		return true
	else
		ya.err("SetPreviewMode - Failed to update preview mode.")
		return false
	end
end

local M = {}

-- Preload function: outputs the query result to a parquet file (cache) using DuckDB's COPY.
function M:preload(job)
	local cache_standard = get_cache_path(job, "standard")
	local cache_summarized = get_cache_path(job, "summarized")
	if not cache_standard or not cache_summarized then
		ya.err("Preload - Could not compute cache paths.")
		return false
	end

	-- If both caches exist, skip preload entirely
	if fs.cha(cache_standard) and fs.cha(cache_summarized) then
		ya.dbg("Preload - Both caches already exist.")
		return true
	end

	-- Run queries separately using create cache
	local success = true
	success = create_cache(job, "standard", cache_standard) and success
	success = create_cache(job, "summarized", cache_summarized) and success
	return success
end

-- Peek Function
function M:peek(job)
	local raw_skip = job.skip or 0
	local skip = math.max(0, raw_skip - 50)

	ya.dbg(string.format("Peek - raw_skip: %d, adjusted skip: %d", raw_skip, skip))

	-- Toggle mode if within special range
	if raw_skip > 0 and raw_skip < 50 then
		local current_mode = get_preview_mode(job)
		local new_mode = current_mode == "standard" and "summarized" or "standard"
		set_preview_mode(job, new_mode)
		skip = 0
	end

	job.skip = skip

	local mode = get_preview_mode(job)
	local cache = get_cache_path(job, mode)
	local file_url = job.file.url
	local target = cache

	local limit = job.area.h - 7
	local offset = skip

	ya.dbg(string.format("Peek - LIMIT: %d, OFFSET: %d", limit, offset))

	if not cache or not fs.cha(cache) then
		ya.dbg("Peek - Cache not found on disk, querying file directly.")
		target = file_url
	end

	local query = generate_query(target, job, limit, offset)
	ya.dbg("Peek - Generated SQL:\n" .. query)

	local output = run_query(job, query, target)

	if not output or output.stdout == "" then
		ya.err("Peek - DuckDB returned no output from target: " .. tostring(target))
		if target ~= file_url then
			target = file_url
			query = generate_query(target, job, limit, offset)
			output = run_query(job, query, target)
			if not output or output.stdout == "" then
				return require("code"):peek(job)
			end
		else
			return require("code"):peek(job)
		end
	end

	ya.preview_widgets(job, { ui.Text.parse(output.stdout):area(job.area) })
end

-- Seek function.
function M:seek(job)
	ya.dbg("Seek - file:" .. tostring(job.file.url))
	ya.dbg("Seek - mtime:" .. tostring(job.file.cha.mtime))

	local OFFSET_BASE = 50

	-- Decode skip from cx (which has the encoded value)
	local encoded_current_skip = cx.active.preview.skip or 0
	local current_skip = math.max(0, encoded_current_skip - OFFSET_BASE)
	local units = job.units or 0

	ya.dbg(string.format("Seek - encoded skip from cx: %d", encoded_current_skip))
	ya.dbg(string.format("Seek - decoded current skip: %d", current_skip))
	ya.dbg(string.format("Seek - job.units: %d", units))

	local new_skip = current_skip + units
	local encoded_skip = new_skip + OFFSET_BASE

	ya.dbg(string.format("Seek - computed new_skip: %d", new_skip))
	ya.dbg(string.format("Seek - sending encoded skip: %d", encoded_skip))

	ya.manager_emit("peek", { encoded_skip, only_if = job.file.url })
end

return M
