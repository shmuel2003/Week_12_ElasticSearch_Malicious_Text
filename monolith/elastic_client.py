from elasticsearch import Elasticsearch
from config import ES_HOST

es = Elasticsearch(ES_HOST, request_timeout=30)