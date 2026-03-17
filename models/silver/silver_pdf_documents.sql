# One final row per document — the Silver table
# ─────────────────────────────────────

{{ config(materialized='table') }}

WITH cleaned AS (
    SELECT * FROM {{ ref('int_pdf_cleaned') }}
),
aggregated AS (
    SELECT
        extract_id,
        source_file,
        extracted_at,
        page_count,
        file_hash,
        source_type,

        -- Reassemble full document text in page order
        LISTAGG(clean_text, ' ')
          WITHIN GROUP (ORDER BY page_num)      AS full_document_text,

        -- Quality metrics
        SUM(char_count)                          AS total_char_count,
        COUNT_IF(is_likely_blank)                AS blank_page_count,
        COUNT_IF(has_substantial_content)        AS content_page_count,
        COUNT_IF(has_tables)                     AS pages_with_tables,
        MAX(table_count)                         AS max_tables_on_page,

        CURRENT_TIMESTAMP()                      AS silver_loaded_at

    FROM cleaned
    GROUP BY 1,2,3,4,5,6
)
SELECT * FROM aggregated