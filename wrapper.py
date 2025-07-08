# wrapper.py
from flask import Flask, request
from sync_airtable import main

app = Flask(__name__)

@app.route("/", methods=["POST"])
def handler():
    # Cloud Run will POST the Pub/Sub push message body here
    main()
    return "", 204

if __name__ == "__main__":
    # allows local testing: just run `python wrapper.py`
    main()
