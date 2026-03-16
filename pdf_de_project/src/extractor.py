import pdfplumber
import hashlib
import json
import os
from datetime import datetime, timezone

RAW_DIR = "data/raw"
PROCESSED_DIR = "data/processed"

def extract_pdf(file_path: str) -> dict:
    """Extract all text and tables from a PDF file."""
    pages = []
    with pdfplumber.open(file_path) as pdf:
        for i, page in enumerate(pdf.pages):
            text = page.extract_text() or ""
            tables = page.extract_tables() or []
            pages.append({
                "page_num": i + 1,
                "text": text,
                "char_count": len(text),
                "table_count": len(tables),
                "tables": [{"rows": t} for t in tables if t]
            })

    # Generate Hash for Data Integrity
    with open(file_path, "rb") as f:
        raw_bytes = f.read()
    file_hash = hashlib.md5(raw_bytes).hexdigest()

    return {
        "source_file": os.path.basename(file_path),
        "file_path": file_path,
        "extracted_at": datetime.now(timezone.utc).isoformat(),
        "page_count": len(pages),
        "total_chars": sum(p["char_count"] for p in pages),
        "file_hash": file_hash,
        "pages": pages
    }

def save_json(data: dict):
    """Save extraction result as JSON in data/processed."""
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    # Filename: hash_originalName.json
    fname = f"{data['file_hash'][:8]}_{data['source_file'].replace('.pdf', '.json')}"
    path = os.path.join(PROCESSED_DIR, fname)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    return path

if __name__ == "__main__":
    # Check for PDFs in the raw folder
    files = [f for f in os.listdir(RAW_DIR) if f.endswith('.pdf')]
    
    if not files:
        print(f"No PDFs found in {RAW_DIR}")
    else:
        print(f"🚀 Found {len(files)} files. Starting Pipeline...")
        for filename in files:
            full_path = os.path.join(RAW_DIR, filename)
            try:
                result = extract_pdf(full_path)
                out_path = save_json(result)
                print(f"✅ Processed: {filename} -> {out_path}")
            except Exception as e:
                print(f"❌ Error processing {filename}: {e}")