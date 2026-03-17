# Cleans text and flags problem pages
# ─────────────────────────────────────

{{ config(materialized='table') }}

WITH staged AS (
    SELECT * FROM {{ ref('stg_pdf_pages') }}
),
cleaned AS (
    SELECT
        extract_id,
        source_file,
        extracted_at,
        page_count,
        page_num,
        file_hash,
        source_type,
        table_count,
        raw_tables,

        -- Clean: strip excess whitespace
        TRIM(REGEXP_REPLACE(raw_text, '\\s+', ' ')) AS clean_text,
        char_count,

        -- Quality flags
        CASE WHEN char_count < 50
             THEN TRUE ELSE FALSE END          AS is_likely_blank,
        CASE WHEN char_count > 500
             THEN TRUE ELSE FALSE END          AS has_substantial_content,
        CASE WHEN table_count > 0
             THEN TRUE ELSE FALSE END          AS has_tables

    FROM staged
    WHERE raw_text IS NOT NULL
)
SELECT * FROM cleaned
