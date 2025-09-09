# diag_feed.py
# -*- coding: utf-8 -*-
import sys, json
from amazon_client import AmazonClient

if len(sys.argv) < 2:
    print("Uso: python diag_feed.py <FEED_ID>")
    sys.exit(1)

feed_id = sys.argv[1]
cli = AmazonClient(simulate=False)

j = cli.get_feed(feed_id)
print("Status:", j.get("processingStatus"))
print(json.dumps(j, ensure_ascii=False, indent=2))

doc_id = j.get("resultFeedDocumentId")
if not doc_id:
    print("Ainda sem resultFeedDocumentId (tenta outra vez em alguns minutos).")
    sys.exit(0)

txt = cli.download_report_text(doc_id)
print("\n----- Processing Report (primeiras 500 linhas) -----\n")
print("\n".join(txt.splitlines()[:500]))
