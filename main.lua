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

local function get_cache_paths(job, mode)
	local skip = job.skip
	job.skip = 0

	local base = ya.file_cache(job)

	job.skip = skip
	if not base then
		return nil
	end

	local suffix = mode == "summarized" and "_summarized.db" or "_standard.db"
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

	ya.dbg("Running DuckDB query on: " .. tostring(job.file.url.name))
	ya.dbg("Running DuckDB query with args: " .. table.concat(args, " "))

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

local function ensure_cache(job, mode, path)
	if fs.cha(path) then
		return true
	end

	ya.dbg("Preload - Creating " .. mode .. " cache at: " .. tostring(path))
	local sql = string.format("CREATE TABLE My_table AS (%s);", generate_sql(job, mode))
	local out = run_query(job, sql, path)
	if not out then
		ya.err("Preload - Failed to generate " .. mode .. " cache.")
		return false
	else
		ya.dbg("Preload - " .. mode:sub(1, 1):upper() .. mode:sub(2) .. " cache created.")
		return true
	end
end

local function generate_query(target, job, limit, mode, offset)
	if target == job.file.url then
		if mode == "standard" then
			return string.format("SELECT * FROM '%s' LIMIT %d OFFSET %d;", tostring(target), limit, offset)
		else
			local query = generate_sql(job, mode)
			return string.format("WITH query as (%s) SELECT * FROM query LIMIT %d OFFSET %d;", query, limit, offset)
		end
	else
		return string.format("SELECT * FROM My_table LIMIT %d OFFSET %d;", limit, offset)
	end
end

local M = {}

-- Preload function: outputs the query result to a parquet file (cache) using DuckDB's COPY.
function M:preload(job)
	ya.dbg("Preload - Entered preload function for: " .. tostring(job.file.url))
	local cache_standard = get_cache_paths(job, "standard")
	local cache_summarized = get_cache_paths(job, "summarized")
	if not cache_standard or not cache_summarized then
		ya.err("Preload - Could not compute cache paths.")
		return false
	end

	-- Derive separate cache paths for each mode
	ya.dbg("Standard cache:" .. tostring(cache_standard))
	ya.dbg("Summarized cache:" .. tostring(cache_summarized))
	local cha = fs.cha(cache_standard)
	ya.dbg("Preload - cha_standard: " .. tostring(cha))
	cha = fs.cha(cache_summarized)
	ya.dbg("Preload - cha_summarized: " .. tostring(cha))

	-- If both caches exist, skip preload entirely
	if fs.cha(cache_standard) and fs.cha(cache_summarized) then
		ya.dbg("Preload - Both caches already exist.")
		return true
	end

	-- Run queries separately using run_query()
	local success = true
	success = ensure_cache(job, "standard", cache_standard) and success
	success = ensure_cache(job, "summarized", cache_summarized) and success
	return success
end

-- Peek Function
function M:peek(job)
	-- store cache and mode variables.

	local mode = tostring(os.getenv("DUCKDB_PREVIEW_MODE") or "standard"):lower()
	if mode ~= "standard" and mode ~= "summarized" then
		mode = "standard"
	end

	local cache = get_cache_paths(job, mode)
	local file_url = job.file.url
	local target = cache

	-- store offset and limit variables
	local limit = job.area.h - 7
	local offset = job.skip or 0
	ya.dbg("Peek - file: " .. tostring(file_url))
	ya.dbg("Peek - Cache path: " .. tostring(cache))
	ya.dbg("Peek - Limit: " .. tostring(limit) .. ", Offset: " .. tostring(offset))

	-- if no cache url use default previewer.
	if not cache then
		ya.err("Peek - No cache url found. Querying file directly.")
		target = file_url
	end

	-- If the cache does not exist yet, query file directly.
	if not fs.cha(cache) then
		ya.dbg("Peek - Cache not found on disk, Querying file directly.")
		target = file_url
	end

	-- Generate and run query.
	local query = generate_query(target, job, limit, mode, offset)
	ya.dbg("Peek - First query:" .. tostring(query))
	local output = run_query(job, query, target)

	-- If query returns no output then use standard previewer.
	if not output or output.stdout == "" then
		ya.err("Peek - duckdb returned no output")
		ya.dbg("Peek - target:" .. tostring(target))
		if target ~= file_url then
			target = file_url

			-- Generate and run query.
			query = generate_query(target, job, limit, mode, offset)
			ya.dbg("Peek - Second query:" .. tostring(query))
			output = run_query(job, query, target)
			if not output or output.stdout == "" then
				return require("code"):peek(job)
			end
		else
			return require("code"):peek(job)
		end
	end

	-- If query returns data, log success and preview.
	ya.dbg("Peek - Query succesfully returned data.")
	ya.preview_widgets(job, { ui.Text.parse(output.stdout):area(job.area) })
end

-- Seek function.
function M:seek(job)
	ya.dbg("Seek - file:" .. tostring(job.file.url))
	ya.dbg("Seek - mtime:" .. tostring(job.file.cha.mtime))
	local h = cx.active.current.hovered
	if not h or h.url ~= job.file.url then
		return
	end
	local current_skip = cx.active.preview.skip or 0
	local new_skip = math.max(0, current_skip + job.units)
	ya.manager_emit("peek", { new_skip, only_if = job.file.url })
end

return M
