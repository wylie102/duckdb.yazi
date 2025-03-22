local M = {}

function M:peek(job)
	-- Construct the DuckDB command to summarize the CSV file.
	local child = Command("duckdb")
		:args({
			"-box",
			"-c",
			[[
    SELECT
        column_name AS column,
        column_type AS type,
        count,
        approx_unique AS unique,
        null_percentage AS null,


        -- MIN/MAX (left as-is; good for both numeric and timestamps)
        LEFT(min, 10) AS min,
        LEFT(max, 10) AS max,

        -- AVG: Skip numeric summaries for time-based columns
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

        -- STD: Skip numeric summaries for time-based columns
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

        -- Quartiles: Skip numeric summaries for time-based columns
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

    FROM (summarize FROM ']]
				.. tostring(job.file.url)
				.. [[');
    ]],
		})
		:stdout(Command.PIPED)
		:stderr(Command.PIPED)
		:spawn()

	-- Fallback if the DuckDB command fails to spawn.
	if not child then
		return require("code"):peek(job)
	end

	-- Capture the command's output.
	local output, err = child:wait_with_output()
	if err then
		ya.err("DuckDB command error: " .. tostring(err))
		return require("code"):peek(job)
	end

	-- Process the standard output.
	local lines = output.stdout
	if lines == "" then
		ya.err("DuckDB returned no output.")
		return require("code"):peek(job)
	end

	-- Display the output in the preview pane.
	ya.preview_widgets(job, { ui.Text.parse(lines):area(job.area) })
end

function M:seek(job)
	require("code"):seek(job)
end

return M
