import psycopg2
from dotenv import load_dotenv
import os, sys, json, time
from pathlib import Path
import logging
load_dotenv()
from .utils.save_to_s3 import _save_to_s3


S3_BUCKET = os.getenv("S3_BUCKET")
S3_REGION = os.getenv("AWS_REGION")
S3_PREFIX = os.getenv("S3_PREFIX", "sumo-api-calls/")
    

webhook = {
  "received_at": 1756357625,
  "type": "endBasho",
  "headers": {
    "Host": "74de6cbafcff.ngrok-free.app",
    "User-Agent": "Go-http-client/2.0",
    "Content-Length": "1508",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json",
    "X-Forwarded-For": "5.78.73.189",
    "X-Forwarded-Host": "74de6cbafcff.ngrok-free.app",
    "X-Forwarded-Proto": "https",
    "X-Webhook-Signature": "da0c5ab394ec7ccc1966b464a010091b05a10320c845cc80b48760c023f12f7c"
  },
  "raw": {
    "type": "endBasho",
    "payload": "eyJkYXRlIjoiMjAyMzExIiwibG9jYXRpb24iOiJGdWt1b2thLCBGdWt1b2thIEludGVybmF0aW9uYWwgQ2VudGVyIiwic3RhcnREYXRlIjoiMjAyMy0xMS0xMlQwMDowMDowMFoiLCJlbmREYXRlIjoiMjAyMy0xMS0yNlQwMDowMDowMFoiLCJ5dXNobyI6W3sidHlwZSI6Ik1ha3V1Y2hpIiwicmlraXNoaUlkIjo3LCJzaGlrb25hRW4iOiJLaXJpc2hpbWEgVGV0c3VvIiwic2hpa29uYUpwIjoi6Zyn5bO244CA6ZC15YqbIn0seyJ0eXBlIjoiSnVyeW8iLCJyaWtpc2hpSWQiOjgsInNoaWtvbmFFbiI6IktvdG9zaG9obyBZb3NoaW5hcmkiLCJzaGlrb25hSnAiOiLnkLTli53ls7DjgIDlkInmiJAifSx7InR5cGUiOiJNYWt1c2hpdGEiLCJyaWtpc2hpSWQiOjYwOSwic2hpa29uYUVuIjoiU2F0b3J1ZnVqaSBUZXBwZWkiLCJzaGlrb25hSnAiOiLogZblr4zlo6so44GV44Go44KL44G144GYKSJ9LHsidHlwZSI6IlNhbmRhbm1lIiwicmlraXNoaUlkIjoyMzYsInNoaWtvbmFFbiI6IkRhaXNob3J5dSBIYXJ1Y2hpa2EiLCJzaGlrb25hSnAiOiLlpKfmmIfpvo0o44Gg44GE44GX44KH44GG44KK44KF44GGKSJ9LHsidHlwZSI6IkpvbmlkYW4iLCJyaWtpc2hpSWQiOjQ5OCwic2hpa29uYUVuIjoiRGFpcmluemFuIFJpbiIsInNoaWtvbmFKcCI6IuWkp+WHnOWxsSjjgaDjgYTjgorjgpPjgZbjgpMpIn0seyJ0eXBlIjoiSm9ub2t1Y2hpIiwicmlraXNoaUlkIjo4ODU0LCJzaGlrb25hRW4iOiJBb25pc2hpa2kgQXJhdGEiLCJzaGlrb25hSnAiOiLlronpnZLpjKbjgIDmlrDlpKcifV0sInNwZWNpYWxQcml6ZXMiOlt7InR5cGUiOiJLYW50by1zaG8iLCJyaWtpc2hpSWQiOjExLCJzaGlrb25hRW4iOiJJY2hpeWFtYW1vdG8gRGFpa2kiLCJzaGlrb25hSnAiOiLkuIDlsbHmnKzjgIDlpKfnlJ8ifSx7InR5cGUiOiJLYW50by1zaG8iLCJyaWtpc2hpSWQiOjIwLCJzaGlrb25hRW4iOiJLb3RvemFrdXJhIE1hc2FrYXRzdSIsInNoaWtvbmFKcCI6IueQtOaru+OAgOWwhuWCkSJ9LHsidHlwZSI6IkthbnRvLXNobyIsInJpa2lzaGlJZCI6NzQsInNoaWtvbmFFbiI6IkF0YW1pZnVqaSBTYWt1dGFybyIsInNoaWtvbmFKcCI6IueGsea1t+WvjOWjq+OAgOaclOWkqumDjiJ9XX0="
  },
  "payload_decoded": {
    "date": "202311",
    "location": "Fukuoka, Fukuoka International Center",
    "startDate": "2023-11-12T00:00:00Z",
    "endDate": "2023-11-26T00:00:00Z",
    "yusho": [
      {
        "type": "Makuuchi",
        "rikishiId": 7,
        "shikonaEn": "Kirishima Tetsuo",
        "shikonaJp": "霧島　鐵力"
      },
      {
        "type": "Juryo",
        "rikishiId": 8,
        "shikonaEn": "Kotoshoho Yoshinari",
        "shikonaJp": "琴勝峰　吉成"
      },
      {
        "type": "Makushita",
        "rikishiId": 609,
        "shikonaEn": "Satorufuji Teppei",
        "shikonaJp": "聖富士(さとるふじ)"
      },
      {
        "type": "Sandanme",
        "rikishiId": 236,
        "shikonaEn": "Daishoryu Haruchika",
        "shikonaJp": "大昇龍(だいしょうりゅう)"
      },
      {
        "type": "Jonidan",
        "rikishiId": 498,
        "shikonaEn": "Dairinzan Rin",
        "shikonaJp": "大凜山(だいりんざん)"
      },
      {
        "type": "Jonokuchi",
        "rikishiId": 8854,
        "shikonaEn": "Aonishiki Arata",
        "shikonaJp": "安青錦　新大"
      }
    ],
    "specialPrizes": [
      {
        "type": "Kanto-sho",
        "rikishiId": 11,
        "shikonaEn": "Ichiyamamoto Daiki",
        "shikonaJp": "一山本　大生"
      },
      {
        "type": "Kanto-sho",
        "rikishiId": 20,
        "shikonaEn": "Kotozakura Masakatsu",
        "shikonaJp": "琴櫻　将傑"
      },
      {
        "type": "Kanto-sho",
        "rikishiId": 74,
        "shikonaEn": "Atamifuji Sakutaro",
        "shikonaJp": "熱海富士　朔太郎"
      }
    ]
  }
}

if webhook['type'] == 'endBasho':
    print("New basho announced:", webhook['payload_decoded']['startDate'], "at", webhook['payload_decoded']['location'])
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT", 5432),
        )
        
        
        cur = conn.cursor()
        basho_query = """
        UPDATE basho
        SET makuuchi_yusho = %s, juryo_yusho = %s, makushita_yusho = %s, sandanme_yusho = %s, jonidan_yusho = %s, jonokuchi_yusho = %s
        WHERE id = %s
        """
        yusho_query = """
        UPDATE rikishi
        SET yusho_count = yusho_count + 1
        WHERE id = %s
        """
        
        for yusho in webhook['payload_decoded']['yusho']:
            cur.execute(yusho_query, (yusho['rikishiId'],))
        
        makuuchi_yusho = webhook['payload_decoded']['yusho'][0]['rikishiId']
        juryo_yusho = webhook['payload_decoded']['yusho'][1]['rikishiId']
        makushita_yusho = webhook['payload_decoded']['yusho'][2]['rikishiId']
        sandanme_yusho = webhook['payload_decoded']['yusho'][3]['rikishiId']
        jonidan_yusho = webhook['payload_decoded']['yusho'][4]['rikishiId']
        jonokuchi_yusho = webhook['payload_decoded']['yusho'][5]['rikishiId']
        
        
        cur.execute(basho_query, (
            makuuchi_yusho,
            juryo_yusho,
            makushita_yusho,
            sandanme_yusho,
            jonidan_yusho,
            jonokuchi_yusho,
            webhook['payload_decoded']['date']
        ))
        conn.commit()
        
        special_prize_query = """
        INSERT INTO special_prizes (basho_id, rikishi_id, prize_name)
        VALUES (%s, %s, %s)"""
        
        sansho_query = """
        UPDATE rikishi
        SET sansho_count = sansho_count + 1
        WHERE id = %s
        """
        
        for prize in webhook['payload_decoded']['specialPrizes']:
            cur.execute(special_prize_query, (
                webhook['payload_decoded']['date'],
                prize['rikishiId'],
                prize['type']
            ))
            cur.execute(sansho_query, (prize['rikishiId'],))
        cur.close()
        conn.close()
        print("Basho event recorded in database.")
        
        
        basho_file = webhook['payload_decoded']
        _save_to_s3(basho_file, S3_PREFIX + "basho", f"basho_{webhook['payload_decoded']['date']}")
        
        print("insert webhook payload into s3 completed.")
        
    except Exception as e:
        print("Database error:", e)
    
    