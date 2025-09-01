import csv
from elastic_client import es
from config import INDEX, PIPELINE, STATUS_INDEX

def load_weapon_list(path: str):
    """
    Load weapons list from a text file (one weapon per line).
    Will return a list of lowercase strings.
    """
    with open(path, "r", encoding="utf-8") as f:
        weapons = [line.strip().lower() for line in f if line.strip()]
    return weapons

def create_ingest_pipeline(weapons: list):
    """
    Create ingest pipeline dynamically with weapon detection and sentiment analysis.
    Sentiment detection is based on simple word count (negWords vs posWords).
    Weapon detection matches against the list provided from weapon_list.txt.
    """
    script_source = """
        String txt = ctx.text != null ? ctx.text.toLowerCase() : "";
        String[] negWords = ['kill','murder','hate','destroy','attack','bomb','shoot'];
        String[] posWords = ['love','like','support','good','happy'];
        int neg=0; int pos=0;
        for (w in negWords) { if (txt.indexOf(w) != -1) neg++; }
        for (w in posWords) { if (txt.indexOf(w) != -1) pos++; }
        if (neg > pos) { ctx.sentiment = 'negative'; }
        else if (pos > neg) { ctx.sentiment = 'positive'; }
        else { ctx.sentiment = 'neutral'; }

        def found = [];
        for (w in params.weapons) {
            if (txt.contains(w)) {
                found.add(w);
            }
        }
        ctx.weapons = found;
    """
    body = {
        "description": "Detect sentiment and weapons",
        "processors": [
            {
                "script": {
                    "lang": "painless",
                    "params": {"weapons": weapons},
                    "source": script_source
                }
            }
        ]
    }
    es.ingest.put_pipeline(id=PIPELINE, body=body)

def bulk_upload_csv(csv_path):
    """
    Read a CSV file with columns: text,label,timestamp
    and bulk index it into Elasticsearch using the ingest pipeline.
    """
    from elasticsearch.helpers import bulk
    actions = []
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            doc = {
                "_op_type": "index",
                "_index": INDEX,
                "_source": {
                    "text": row.get("text"),
                    "label": row.get("label"),
                    "timestamp": row.get("timestamp")
                }
            }
            actions.append(doc)
            if len(actions) >= 500:
                bulk(es, actions, pipeline=PIPELINE)
                actions = []
        if actions:
            bulk(es, actions, pipeline=PIPELINE)

def set_processing_status(done: bool):
    """
    Save a simple status doc (done / not done) into Elasticsearch
    """
    body = {"done": done}
    es.index(index=STATUS_INDEX, id="status", document=body)