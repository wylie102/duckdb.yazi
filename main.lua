local M = {}

-- Returns a path to the DuckDB file in the system temp dir
local function get_db_path()
	local temp = os.getenv("TMPDIR") or os.getenv("TEMP") or os.getenv("TMP") or "/tmp"
	if temp:sub(-1) == "/" or temp:sub(-1) == "\\" then
		temp = temp:sub(1, -2)
	end
	return temp .. "/yazi.duckdb"
end

local function initialize_cache_db()
	local db_path = get_db_path()
  if not fs.cha(db_path) then
    local args = { "-c"}
    args[#args+1] = string.format("ATTACH IF NOT EXISTS '%s' (STORAGE_VERSION 'v1.2.0');", db_path)
    local cmd = Command("duckdb"):args(args):stdout(Command.PIPED):stderr(Command.PIPED):spawn()
      if not cmd then
        return nil
      end
    return db_path
  end

-- Runs DuckDB query.
local function run_duckdb(db_path, sql)
	local args = { db_path }
	args[#args + 1] = "-c"
	args[#args + 1] = sql
	local cmd = Command("duckdb"):args(args):stdout(Command.PIPED):stderr(Command.PIPED):spawn()
	if not cmd then
		return nil
	end
	return cmd
end


-- Makes a safe schema name from (file_url)
local function get_schema_name(file_url)
    local url_string = tostring(file_url)
    local hashed_url = ya.hash(url_string)
    return hashed_url
end

-- This function generates the SQL query based on the preview mode.
local function generate_initial_query(job, mode)
	local initial_query = ""
	if mode == "standard" then
		initial_query = string.format("SELECT * FROM '%s' LIMIT 500", tostring(job.file.url))
	else
		initial_query = string.format(
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
	return initial_query
end

-- Preload function: outputs the query result to a parquet file (cache) using DuckDB's COPY.
function M:preload(job)
	local file_url = job.file and job.file.url
	if not file_url then
		ya.dbg("Preload - No file url found.")
		return false
	end

  local db_path = initialize_cache_db()

  --Check if schema exists, if not then create schema 


  -- create or replace schema in db.
  local schema_name = get_schema_name(file_url)
  local schema_create = string.format("CREATE OR REPLACE SCHEMA (%s);", schema_name)
  run_duckdb(db_path, sql)

	-- Generate basic sql statements.
	local standard_sql = generate_initial_query(job, "standard")
	local summarized_sql = generate_initial_query(job, "summarized")
	ya.dbg("Preload -  basic query statements returned.")

	-- Generate create table statements.
	local create_table_standard = string.format("CREATE TABLE (%s).Standard AS (%s);", schema_name, standard_sql)
	local create_table_summarized = string.format("CREATE TABLE (%s).Summarized AS (%s);", schema_name, summarized_sql)
	ya.dbg("Preload - create table statements returned.")

	-- Sleep for 0.01s to avoid blocking peek process on first file highlighted.
	ya.sleep(0.01)

	-- Create database and run queries to create tables.
	local child = Command("duckdb")
		:args({ tostring(cache), "-c", create_table_summarized, "-c", create_table_standard })
		:stdout(Command.PIPED)
		:stderr(Command.PIPED)
		:spawn()
	if not child then
		ya.dbg("Preload - Failed to initialise db and store created tables.")
		return false
	end

	-- Wait for completion and return any errors.
	local output, err = child:wait_with_output()
	if err or not output or (output.stderr and output.stderr ~= "") then
		ya.err("DuckDB preloader error: " .. (err or output.stderr))
		return false
	end

	-- tables created.
	ya.dbg("Preload - Db and tables created.")
	return true
end

-- Peek Function
function M:peek(job)
	-- store cache and mode variables.
	local cache = ya.file_cache(job)
	local mode = os.getenv("DUCKDB_PREVIEW_MODE") or "summarized"

	-- if no cache url use default previewer.
	if not cache then
		ya.dbg("Peek - No cache url found. Using default preview.")
		return require("code"):peek(job)
	end

	-- echo cache path to log.
	ya.dbg("Peek - Cache path: " .. tostring(cache))

	-- If the cache does not exist yet, try preloading.
	if not fs.cha(cache) then
		ya.dbg("Peek - Cache not found on disk, attempting to preload")
		if not self:preload(job) then
			ya.dbg("Peek - Preload failed. Using default preview")
			return require("code"):peek(job)
		end
	end

	-- Store and lof limit and offset variables.
	local limit = job.area.h - 2
	local offset = job.skip or 0
	ya.dbg("Peek - Limit: " .. tostring(limit) .. ", Offset: " .. tostring(offset))

	-- store table name variable.
	local queried_table = ""
	if mode == "standard" then
		queried_table = "Standard"
	else
		queried_table = "Summarized"
	end

	-- Generate query.
	local query = string.format("SELECT * FROM %s LIMIT %d OFFSET %d;", queried_table, limit, offset)
	ya.dbg("Peek - SQL Query: " .. query)

	-- Run query.
	local child =
		Command("duckdb"):args({ tostring(cache), "-c", query }):stdout(Command.PIPED):stderr(Command.PIPED):spawn()

	-- If query fails use standerd preview.
	if not child then
		ya.dbg("Peek - Failed to spawn DuckDB command, falling back")
		return require("code"):peek(job)
	end

	-- Wait on result, if error use standard previewer.
	local output, err = child:wait_with_output()
	if err then
		ya.dbg("DuckDB command error: " .. tostring(err))
		return require("code"):peek(job)
	end

	-- If query returns no output then use standard previewer.
	if output.stdout == "" then
		ya.dbg("DuckDB returned no output.")
		return require("code"):peek(job)
	end

	-- If query returns data, log success and preview.
	ya.dbg("Peek - Query succesfully returned data.")
	ya.preview_widgets(job, { ui.Text.parse(output.stdout):area(job.area) })
end

-- Seek function.
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
