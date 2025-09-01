import os

ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
INDEX = "malicious_texts"
STATUS_INDEX = "processing_status"
PIPELINE = "malicious_pipeline"