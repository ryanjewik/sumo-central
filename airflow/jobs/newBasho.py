import psycopg2
from dotenv import load_dotenv
import os, sys, json, time
from pathlib import Path
load_dotenv()

webhook = {
  "received_at": 1756357624,
  "type": "newBasho",
  "headers": {
    "Host": "74de6cbafcff.ngrok-free.app",
    "User-Agent": "Go-http-client/2.0",
    "Content-Length": "216",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json",
    "X-Forwarded-For": "5.78.73.189",
    "X-Forwarded-Host": "74de6cbafcff.ngrok-free.app",
    "X-Forwarded-Proto": "https",
    "X-Webhook-Signature": "51d5e8f38a6624f8ff413419328855ad419c79f4d183c0544f2441a592895533"
  },
  "raw": {
    "type": "newBasho",
    "payload": "eyJkYXRlIjoiMjAyMzExIiwibG9jYXRpb24iOiJGdWt1b2thLCBGdWt1b2thIEludGVybmF0aW9uYWwgQ2VudGVyIiwic3RhcnREYXRlIjoiMjAyMy0xMS0xMlQwMDowMDowMFoiLCJlbmREYXRlIjoiMjAyMy0xMS0yNlQwMDowMDowMFoifQ=="
  },
  "payload_decoded": {
    "date": "202311",
    "location": "Fukuoka, Fukuoka International Center",
    "startDate": "2023-11-12T00:00:00Z",
    "endDate": "2023-11-26T00:00:00Z"
  }
}

if webhook['type'] == 'newBasho':
    print("New basho announced:", webhook['payload_decoded']['startDate'], "at", webhook['payload_decoded']['location'])
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
        )
        cur = conn.cursor()
        insert_query = """
        INSERT INTO basho (
          id, name, location, start_date, end_date, makuuchi_yusho, juryo_yusho, makushita_yusho, jonidan_yusho, sandanme_yusho
          )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.execute(insert_query, (
            webhook['payload_decoded']['date'],
            "Basho " + webhook['payload_decoded']['date'],
            webhook['payload_decoded']['location'],
            webhook['payload_decoded']['startDate'],
            webhook['payload_decoded']['endDate'],
            None, None, None, None, None
        ))
        conn.commit()
        cur.close()
        conn.close()
        print("Basho event recorded in database.")
    except Exception as e:
        print("Database error:", e)
    
    