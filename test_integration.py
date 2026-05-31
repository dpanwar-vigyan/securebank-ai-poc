import os
from dotenv import load_dotenv
load_dotenv()                                        # load .env before any imports that read env vars
os.environ["ANALYTICS_DB_BACKEND"] = "motherduck"   # override: test MotherDuck regardless of .env setting
from rag.db_adapters import get_adapter

db = get_adapter()
print("ping:", db.ping())

r = db.query("SELECT COUNT(*) FROM banking_docs.documents")
print("doc count:", r.result_rows)

r = db.query("SELECT doc_id, doc_type FROM banking_docs.documents LIMIT 3")
for row in r.result_rows:
    print(row)
