# plugins/postgres/utils/qrylib/index_bloat_analysis.py

# plugins/postgres/utils/qrylib/index_bloat_analysis.py

def index_bloat_query(connector):
    """
    Returns a query to estimate the amount of bloat in B-Tree indexes.
    This query is adapted from the pgsql-bloat-estimation project and is
    considered a more accurate method.
    Source: https://github.com/ioguix/pgsql-bloat-estimation
    """
    # This query now hardcodes the LIMIT as requested.
    return """
-- Refactored for readability using Common Table Expressions (CTEs)
-- Original logic from https://github.com/ioguix/pgsql-bloat-estimation
WITH idx_data AS (
    SELECT
        ci.relname AS idxname,
        ci.reltuples,
        ci.relpages,
        i.indrelid AS tbloid,
        i.indexrelid AS idxoid,
        coalesce(substring(array_to_string(ci.reloptions, ' ') from 'fillfactor=([0-9]+)')::smallint, 90) AS fillfactor,
        i.indnatts,
        pg_catalog.string_to_array(pg_catalog.textin(pg_catalog.int2vectorout(i.indkey)),' ')::int[] AS indkey
    FROM pg_catalog.pg_index i
    JOIN pg_catalog.pg_class ci ON ci.oid = i.indexrelid
    WHERE ci.relam=(SELECT oid FROM pg_am WHERE amname = 'btree') AND ci.relpages > 0
),
ic AS (
    SELECT *, pg_catalog.generate_series(1,indnatts) AS attpos FROM idx_data
),
i AS (
    SELECT
        ct.relname AS tblname,
        ct.relnamespace,
        ic.idxname, ic.attpos, ic.indkey, ic.indkey[ic.attpos], ic.reltuples, ic.relpages, ic.tbloid, ic.idxoid, ic.fillfactor,
        coalesce(a1.attnum, a2.attnum) AS attnum,
        coalesce(a1.attname, a2.attname) AS attname,
        coalesce(a1.atttypid, a2.atttypid) AS atttypid,
        CASE WHEN a1.attnum IS NULL THEN ic.idxname ELSE ct.relname END AS attrelname
    FROM ic
    JOIN pg_catalog.pg_class ct ON ct.oid = ic.tbloid
    LEFT JOIN pg_catalog.pg_attribute a1 ON ic.indkey[ic.attpos] <> 0 AND a1.attrelid = ic.tbloid AND a1.attnum = ic.indkey[ic.attpos]
    LEFT JOIN pg_catalog.pg_attribute a2 ON ic.indkey[ic.attpos] = 0 AND a2.attrelid = ic.idxoid AND a2.attnum = ic.attpos
),
rows_data_stats AS (
    SELECT
        n.nspname, i.tblname, i.idxname, i.reltuples, i.relpages, i.idxoid, i.fillfactor,
        current_setting('block_size')::numeric AS bs,
        CASE WHEN version() ~ 'mingw32' OR version() ~ '64-bit|x86_64|ppc64|ia64|amd64' THEN 8 ELSE 4 END AS maxalign,
        24 AS pagehdr,
        16 AS pageopqdata,
        CASE WHEN max(coalesce(s.null_frac,0)) = 0 THEN 8 ELSE 8 + (( 32 + 8 - 1 ) / 8) END AS index_tuple_hdr_bm,
        sum( (1-coalesce(s.null_frac, 0)) * coalesce(s.avg_width, 1024)) AS nulldatawidth,
        max( CASE WHEN i.atttypid = 'pg_catalog.name'::regtype THEN 1 ELSE 0 END ) > 0 AS is_na
    FROM i
    JOIN pg_catalog.pg_namespace n ON n.oid = i.relnamespace
    JOIN pg_catalog.pg_stats s ON s.schemaname = n.nspname AND s.tablename = i.attrelname AND s.attname = i.attname
    GROUP BY 1,2,3,4,5,6,7
),
rows_hdr_pdg_stats AS (
    SELECT *,
        (index_tuple_hdr_bm + maxalign - CASE WHEN index_tuple_hdr_bm%maxalign = 0 THEN maxalign ELSE index_tuple_hdr_bm%maxalign END
        + nulldatawidth + maxalign - CASE WHEN nulldatawidth = 0 THEN 0 WHEN nulldatawidth::integer%maxalign = 0 THEN maxalign ELSE nulldatawidth::integer%maxalign END
        )::numeric AS nulldatahdrwidth
    FROM rows_data_stats
),
relation_stats AS (
    SELECT *,
        coalesce(1 + ceil(reltuples/floor((bs-pageopqdata-pagehdr)/(4+nulldatahdrwidth)::float)), 0) AS est_pages,
        coalesce(1 + ceil(reltuples/floor((bs-pageopqdata-pagehdr)*fillfactor/(100*(4+nulldatahdrwidth)::float))), 0) AS est_pages_ff
    FROM rows_hdr_pdg_stats
)
-- MODIFIED: Wrapped the final select to allow filtering on aliased columns
SELECT * FROM (
    SELECT
        current_database(),
        nspname AS schemaname,
        tblname,
        idxname,
        bs*(relpages)::bigint AS real_size,
        bs*(relpages-est_pages)::bigint AS extra_size,
        100 * (relpages-est_pages)::float / relpages AS extra_pct,
        fillfactor,
        CASE WHEN relpages > est_pages_ff THEN bs*(relpages-est_pages_ff) ELSE 0 END AS bloat_size,
        100 * (relpages-est_pages_ff)::float / relpages AS bloat_pct,
        is_na
    FROM relation_stats
) AS final_data
WHERE schemaname !='pg_catalog' AND bloat_pct > 9
ORDER BY bloat_pct, bloat_size ASC
LIMIT 10;
"""
