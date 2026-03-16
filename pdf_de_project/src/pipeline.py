import os
import sys
import logging
from datetime import datetime
from extractor import extract_pdf, save_json
from loader import load_to_bronze

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler(f"logs/pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

def run_pipeline(pdf_dir: str = "data/raw"):
    pdf_files = [f for f in os.listdir(pdf_dir) if f.endswith(".pdf")]

    if not pdf_files:
        log.warning(f"No PDF files found in {pdf_dir}")
        return

    log.info(f"Found {len(pdf_files)} PDFs to process")

    results = {"loaded": 0, "skipped": 0, "error": 0}

    for fname in pdf_files:
        fpath = os.path.join(pdf_dir, fname)
        log.info(f"Processing: {fname}")
        try:
            data = extract_pdf(fpath)
            save_json(data)
            status = load_to_bronze(data)
            results[status] += 1
        except Exception as e:
            log.error(f"Failed on {fname}: {e}")
            results["error"] += 1

    log.info(f"Done. Loaded={results['loaded']} Skipped={results['skipped']} Errors={results['error']}")

if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    directory = sys.argv[1] if len(sys.argv) > 1 else "data/raw"
    run_pipeline(directory)