# main.py
from fastapi import FastAPI
from ingest_pipeline import create_index_and_pipeline
from loader import load_csv_to_es
from cleanup import cleanup_index
from search_api import router
import os

app = FastAPI(title="Week_12_ElasticSearch_Malicious_Text")

app.include_router(router, prefix="/api")

@app.on_event("startup")
def startup_event():
    # Create index & pipeline
    create_index_and_pipeline()
    # Optionally auto-load CSV if env var set
    auto_load = os.environ.get("AUTO_LOAD", "true").lower() in ("1","true","yes")
    if auto_load:
        csv_path = os.environ.get("CSV_PATH", "/app/data/iran_texts.csv")
        load_csv_to_es(csv_path)
        cleanup_index()
