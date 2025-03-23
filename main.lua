-- ~/.config/yazi/plugins/duckdb.yazi/main.lua
local M = {}

-- Simple debug logger: logs only if YAZI_LOG=debug
local function log_debug(...)
	local parts = {}
	for i = 1, select("#", ...) do
		parts[#parts + 1] = tostring(select(i, ...))
	end
	ya.dbg(table.concat(parts, " "))
end

-- Returns a path to the DuckDB file in the system temp dir
local function get_db_path()
	local temp = os.getenv("TMPDIR") or os.getenv("TEMP") or os.getenv("TMP") or "/tmp"
	if temp:sub(-1) == "/" or temp:sub(-1) == "\\" then
		temp = temp:sub(1, -2)
	end
	return temp .. "/yazi.duckdb"
end

-- Runs DuckDB with optional CSV output
local function run_duckdb(sql, opts)
	local args = { get_db_path() }
	if opts and opts.csv then
		args[#args + 1] = "-csv"
		args[#args + 1] = "-header"
	end
	args[#args + 1] = "-c"
	args[#args + 1] = sql

	log_debug("DuckDB =>", table.concat(args, " "))
	local cmd = Command("duckdb"):args(args):stdout(Command.PIPED):stderr(Command.PIPED):spawn()
	if not cmd then
		log_debug("Failed to spawn DuckDB.")
		return nil
	end
	return cmd
end

-- Makes a safe table name from (file_url, mode)
local function get_table_name(file_url, mode)
	if not file_url then
		return "no_file_url_" .. (mode or "unknown")
	end
	local s = tostring(file_url):gsub("[^%w_]+", "_")
	return s .. "_" .. mode
end

-- Query to do the "summarize" in DuckDB 0.7+
local function summarize_query(file_url)
	local path = tostring(file_url)
	return string.format(
		[[
WITH summary AS (
    SELECT
        column_name AS column, column_type AS type, count, approx_unique AS unique,
        null_percentage AS null, LEFT(min, 10) AS min, LEFT(max, 10) AS max,
        CASE WHEN column_type IN ('TIMESTAMP','DATE') THEN '-'
             WHEN avg IS NULL THEN 'NULL'
             WHEN TRY_CAST(avg AS DOUBLE) IS NULL THEN avg
             WHEN CAST(avg AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(avg AS DOUBLE),2) AS VARCHAR)
             WHEN CAST(avg AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(avg AS DOUBLE)/1000,1) AS VARCHAR)||'k'
             WHEN CAST(avg AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(avg AS DOUBLE)/1000000,2) AS VARCHAR)||'m'
             WHEN CAST(avg AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(avg AS DOUBLE)/1000000000,2) AS VARCHAR)||'b'
             ELSE '∞'
        END AS avg,
        CASE WHEN column_type IN ('TIMESTAMP','DATE') THEN '-'
             WHEN std IS NULL THEN 'NULL'
             WHEN TRY_CAST(std AS DOUBLE) IS NULL THEN std
             WHEN CAST(std AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(std AS DOUBLE),2) AS VARCHAR)
             WHEN CAST(std AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(std AS DOUBLE)/1000,1) AS VARCHAR)||'k'
             WHEN CAST(std AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(std AS DOUBLE)/1000000,2) AS VARCHAR)||'m'
             WHEN CAST(std AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(std AS DOUBLE)/1000000000,2) AS VARCHAR)||'b'
             ELSE '∞'
        END AS std,
        CASE WHEN column_type IN ('TIMESTAMP','DATE') THEN '-'
             WHEN q25 IS NULL THEN 'NULL'
             WHEN TRY_CAST(q25 AS DOUBLE) IS NULL THEN q25
             WHEN CAST(q25 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q25 AS DOUBLE),2) AS VARCHAR)
             WHEN CAST(q25 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE)/1000,1) AS VARCHAR)||'k'
             WHEN CAST(q25 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE)/1000000,2) AS VARCHAR)||'m'
             WHEN CAST(q25 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q25 AS DOUBLE)/1000000000,2) AS VARCHAR)||'b'
             ELSE '∞'
        END AS q25,
        CASE WHEN column_type IN ('TIMESTAMP','DATE') THEN '-'
             WHEN q50 IS NULL THEN 'NULL'
             WHEN TRY_CAST(q50 AS DOUBLE) IS NULL THEN q50
             WHEN CAST(q50 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q50 AS DOUBLE),2) AS VARCHAR)
             WHEN CAST(q50 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE)/1000,1) AS VARCHAR)||'k'
             WHEN CAST(q50 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE)/1000000,2) AS VARCHAR)||'m'
             WHEN CAST(q50 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q50 AS DOUBLE)/1000000000,2) AS VARCHAR)||'b'
             ELSE '∞'
        END AS q50,
        CASE WHEN column_type IN ('TIMESTAMP','DATE') THEN '-'
             WHEN q75 IS NULL THEN 'NULL'
             WHEN TRY_CAST(q75 AS DOUBLE) IS NULL THEN q75
             WHEN CAST(q75 AS DOUBLE) < 100000 THEN CAST(ROUND(CAST(q75 AS DOUBLE),2) AS VARCHAR)
             WHEN CAST(q75 AS DOUBLE) < 1000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE)/1000,1) AS VARCHAR)||'k'
             WHEN CAST(q75 AS DOUBLE) < 1000000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE)/1000000,2) AS VARCHAR)||'m'
             WHEN CAST(q75 AS DOUBLE) < 1000000000000 THEN CAST(ROUND(CAST(q75 AS DOUBLE)/1000000000,2) AS VARCHAR)||'b'
             ELSE '∞'
        END AS q75
    FROM (summarize FROM '%s')
)
SELECT * FROM summary
]],
		path
	)
end

-- CREATE TABLE logic
local function create_table_sql(name, file_url, mode)
	if mode == "standard" then
		return string.format("CREATE TABLE %s AS SELECT * FROM '%s' LIMIT 1000;", name, tostring(file_url))
	else
		return string.format("CREATE TABLE %s AS %s;", name, summarize_query(file_url))
	end
end

-- Check existence with CSV for easy parsing
local function table_exists_sql(name)
	return string.format(
		[[
SELECT COUNT(*) as table_count
FROM information_schema.tables
WHERE table_name = '%s';
]],
		name
	)
end

-- We parse lines[1] = "table_count", lines[2] = "0" or "1"
local function parse_count_csv(stdout)
	local lines = {}
	for ln in stdout:gmatch("[^\r\n]+") do
		lines[#lines + 1] = ln
	end
	if #lines >= 2 then
		return tonumber(lines[2]) or 0
	end
	return 0
end

local function select_sql(name, limit, offset)
	return string.format("SELECT * FROM %s LIMIT %d OFFSET %d;", name, limit, offset)
end

function M:peek(job)
	local mode = os.getenv("DUCKDB_PREVIEW_MODE") or "summarize"
	local limit = (job.area.h or 10) - 2
	local offset = job.skip or 0
	local url = job.file and job.file.url
	local tname = get_table_name(url, mode)

	-- Check existence
	local check_cmd = run_duckdb(table_exists_sql(tname), { csv = true })
	if not check_cmd then
		return require("code"):peek(job)
	end

	local check_out, check_err = check_cmd:wait_with_output()
	if (check_err and check_err ~= "") or (check_out.stderr ~= "") then
		ya.err("DuckDB check error: " .. (check_err or check_out.stderr))
		return require("code"):peek(job)
	end

	local count = parse_count_csv(check_out.stdout)
	local exists = (count > 0)
	log_debug("Table", tname, "exists?", exists)

	-- Create if missing
	if not exists then
		local sql = create_table_sql(tname, url, mode)
		log_debug("Creating table =>", sql)
		local ccmd = run_duckdb(sql)
		if not ccmd then
			return require("code"):peek(job)
		end
		local co, ce = ccmd:wait_with_output()
		if (ce and ce ~= "") or (co.stderr ~= "") then
			ya.err("DuckDB create table error: " .. (ce or co.stderr))
			return require("code"):peek(job)
		end
	end

	-- Select from that table
	local sel = select_sql(tname, limit, offset)
	local scmd = run_duckdb(sel)
	if not scmd then
		return require("code"):peek(job)
	end

	local sout, serr = scmd:wait_with_output()
	if (serr and serr ~= "") or (sout.stderr ~= "") then
		ya.err("DuckDB query error: " .. (serr or sout.stderr))
		return require("code"):peek(job)
	end

	if sout.stdout == "" then
		ya.err("DuckDB returned no output.")
		return require("code"):peek(job)
	end

	ya.preview_widgets(job, {
		ui.Text.parse(sout.stdout):area(job.area),
	})
end

function M:seek(job)
	local h = cx.active.current.hovered
	if not h or h.url ~= job.file.url then
		return
	end

	local cur = cx.active.preview.skip or 0
	local new = math.max(0, cur + job.units)
	ya.mgr_emit("peek", { new, only_if = job.file.url })
end

return M
