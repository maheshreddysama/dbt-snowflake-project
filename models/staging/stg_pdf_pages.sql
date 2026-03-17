# Flattens the JSON array into one row per page
# ─────────────────────────────────────

{{ config(materialized='view') }}

WITH source AS (
    SELECT * FROM {{ source('bronze', 'raw_pdf_extracts') }}
),
flattened AS (
    SELECT
        s.extract_id,
        s.source_file,
        s.extracted_at,
        s.page_count,
        s.file_hash,
        s.source_type,
        f.value:page_num::INT          AS page_num,
        f.value:text::STRING           AS raw_text,
        f.value:char_count::INT        AS char_count,
        f.value:table_count::INT       AS table_count,
        f.value:tables                 AS raw_tables
    FROM source s,
    LATERAL FLATTEN(input => s.raw_payload:pages) f
)
SELECT * FROM flattened
