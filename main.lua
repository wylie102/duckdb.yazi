local M = {}

-- This function generates the SQL query based on the preview mode.
local function generate_sql(job, mode)
	local query = ""
	if mode == "standard" then
		query = string.format("SELECT * FROM '%s' LIMIT 500", tostring(job.file.url))
	else
		query = string.format(
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
	return query
end

-- Preload function: outputs the query result to a parquet file (cache) using DuckDB's COPY.
function M:preload(job)
	local cache = ya.file_cache(job)
	if not cache then
		return false
	end

	-- If the cache file already exists, no need to preload.
	if fs.cha(cache) then
		return true
	end

	-- Build the COPY query to output the result to a parquet file.
	local mode = os.getenv("DUCKDB_PREVIEW_MODE") or "summarize"
	local base_sql = generate_sql(job, mode)
	local copy_sql = string.format("COPY (%s) TO '%s' (FORMAT 'parquet');", base_sql, tostring(cache))

	local child = Command("duckdb"):args({ "-c", copy_sql }):stdout(Command.PIPED):stderr(Command.PIPED):spawn()
	if not child then
		return false
	end

	local output, err = child:wait_with_output()
	if err or not output or (output.stderr and output.stderr ~= "") then
		ya.err("DuckDB preloader error: " .. (err or output.stderr))
		return false
	end

	return true
end

function M:peek(job)
	local cache = ya.file_cache(job)
	if not cache then
		return require("code"):peek(job)
	end

	-- If the cache does not exist yet, try preloading.
	if not fs.cha(cache) then
		if not self:preload(job) then
			return require("code"):peek(job)
		end
	end

	local limit = job.area.h - 2
	local offset = job.skip or 0
	-- Query the cached parquet file.
	local sql_query = string.format("SELECT * FROM '%s' LIMIT %d OFFSET %d;", tostring(cache), limit, offset)

	local child =
		Command("duckdb"):args({ "-box", "-c", sql_query }):stdout(Command.PIPED):stderr(Command.PIPED):spawn()
	if not child then
		return require("code"):peek(job)
	end

	local output, err = child:wait_with_output()
	if err then
		ya.err("DuckDB command error: " .. tostring(err))
		return require("code"):peek(job)
	end

	if output.stdout == "" then
		ya.err("DuckDB returned no output.")
		return require("code"):peek(job)
	end

	ya.preview_widgets(job, { ui.Text.parse(output.stdout):area(job.area) })
end

function M:seek(job)
	local h = cx.active.current.hovered
	if not h or h.url ~= job.file.url then
		return
	end
	local current_skip = cx.active.preview.skip or 0
	local new_skip = math.max(0, current_skip + job.units)
	ya.manager_emit("peek", { new_skip, only_if = job.file.url })
end

return M
