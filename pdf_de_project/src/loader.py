import snowflake.connector
import json
import os
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SF_ACCOUNT"),
        user=os.getenv("SF_USER"),
        password=os.getenv("SF_PASSWORD"),
        database=os.getenv("SF_DATABASE"),
        warehouse=os.getenv("SF_WAREHOUSE"),
        role=os.getenv("SF_ROLE"),
        schema="BRONZE"
    )

def is_already_loaded(cur, file_hash: str) -> bool:
    """Check if this file was already loaded (idempotency check)."""
    cur.execute(
        "SELECT COUNT(*) FROM BRONZE.RAW_PDF_EXTRACTS WHERE FILE_HASH = %s",
        (file_hash,)
    )
    return cur.fetchone()[0] > 0

def load_to_bronze(data: dict) -> str:
    """
    Load extracted PDF data into Snowflake Bronze table.
    Returns: 'loaded', 'skipped', or 'error'
    """
    conn = get_connection()
    try:
        cur = conn.cursor()

        # Idempotency: skip if already loaded
        if is_already_loaded(cur, data["file_hash"]):
            print(f"SKIPPED (already exists): {data['source_file']}")
            return "skipped"

        cur.execute("""
            INSERT INTO BRONZE.RAW_PDF_EXTRACTS
                (SOURCE_FILE, EXTRACTED_AT, PAGE_COUNT,
                 RAW_PAYLOAD, FILE_HASH, SOURCE_TYPE)
            SELECT
                %s,
                %s::TIMESTAMP_NTZ,
                %s,
                PARSE_JSON(%s),
                %s,
                %s
        """, (
            data["source_file"],
            data["extracted_at"],
            data["page_count"],
            json.dumps(data),
            data["file_hash"],
            data.get("source_type", "digital")
        ))

        conn.commit()
        print(f"LOADED: {data['source_file']} ({data['page_count']} pages)")
        return "loaded"

    except Exception as e:
        print(f"ERROR loading {data['source_file']}: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    from extractor import extract_pdf
    import sys
    
    # Use the first PDF found in data/raw if no file is specified
    default_dir = "data/raw"
    existing_pdfs = [f for f in os.listdir(default_dir) if f.endswith('.pdf')]
    
    if len(sys.argv) > 1:
        target_pdf = sys.argv[1]
    elif existing_pdfs:
        target_pdf = os.path.join(default_dir, existing_pdfs[0])
    else:
        target_pdf = None

    if target_pdf and os.path.exists(target_pdf):
        print(f"🧪 Testing loader with: {target_pdf}")
        data = extract_pdf(target_pdf)
        load_to_bronze(data)
    else:
        print("❌ No PDF found to test with. Put a file in data/raw!")