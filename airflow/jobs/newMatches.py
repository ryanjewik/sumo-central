from concurrent.futures import ThreadPoolExecutor
import psycopg2
from dotenv import load_dotenv
import os, sys, json, time

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
S3_REGION = os.getenv("AWS_REGION")
S3_PREFIX = os.getenv("S3_PREFIX", "sumo-api-calls/")

webhook = {
  "received_at": 1756357623,
  "type": "newMatches",
  "headers": {
    "Host": "74de6cbafcff.ngrok-free.app",
    "User-Agent": "Go-http-client/2.0",
    "Content-Length": "7470",
    "Accept-Encoding": "gzip",
    "Content-Type": "application/json",
    "X-Forwarded-For": "5.78.73.189",
    "X-Forwarded-Host": "74de6cbafcff.ngrok-free.app",
    "X-Forwarded-Proto": "https",
    "X-Webhook-Signature": "82b3a91f97f1dad97463ac00907b9914adcd80aec013498e5b14c3b339f23c6e"
  },
  "raw": {
    "type": "newMatches",
    "payload": "W3siaWQiOiIyMDIzMTEtMS0xLTY2LTQwIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjEsImVhc3RJZCI6NjYsImVhc3RTaGlrb25hIjoiS2l0YW5vd2FrYSIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSAxNyBFYXN0Iiwid2VzdElkIjo0MCwid2VzdFNoaWtvbmEiOiJOaXNoaWtpZnVqaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSAxNiBXZXN0Iiwia2ltYXJpdGUiOiIiLCJ3aW5uZXJJZCI6MCwid2lubmVyRW4iOiIiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0yLTU1LTcxIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjIsImVhc3RJZCI6NTUsImVhc3RTaGlrb25hIjoiUm9nYSIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSAxNiBFYXN0Iiwid2VzdElkIjo3MSwid2VzdFNoaWtvbmEiOiJDaHVyYW5vdW1pIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDE1IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTMtNTQtMTEiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6MywiZWFzdElkIjo1NCwiZWFzdFNoaWtvbmEiOiJUb2hha3VyeXUiLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMTUgRWFzdCIsIndlc3RJZCI6MTEsIndlc3RTaGlrb25hIjoiSWNoaXlhbWFtb3RvIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDE0IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTQtMTAyLTMxIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjQsImVhc3RJZCI6MTAyLCJlYXN0U2hpa29uYSI6IlRvbW9rYXplIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDE0IEVhc3QiLCJ3ZXN0SWQiOjMxLCJ3ZXN0U2hpa29uYSI6IlRzdXJ1Z2lzaG8iLCJ3ZXN0UmFuayI6Ik1hZWdhc2hpcmEgMTMgV2VzdCIsImtpbWFyaXRlIjoiIiwid2lubmVySWQiOjAsIndpbm5lckVuIjoiIiwid2lubmVySnAiOiIifSx7ImlkIjoiMjAyMzExLTEtNS0yNS0xNCIsImJhc2hvSWQiOiIyMDIzMTEiLCJkaXZpc2lvbiI6Ik1ha3V1Y2hpIiwiZGF5IjoxLCJtYXRjaE5vIjo1LCJlYXN0SWQiOjI1LCJlYXN0U2hpa29uYSI6IlRha2FyYWZ1amkiLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMTMgRWFzdCIsIndlc3RJZCI6MTQsIndlc3RTaGlrb25hIjoiVGFtYXdhc2hpIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDEyIFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTYtNDEtMjQiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6NiwiZWFzdElkIjo0MSwiZWFzdFNoaWtvbmEiOiJPaG8iLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMTIgRWFzdCIsIndlc3RJZCI6MjQsIndlc3RTaGlrb25hIjoiSGlyYWRvdW1pIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDExIFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTctMzUtMzAiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6NywiZWFzdElkIjozNSwiZWFzdFNoaWtvbmEiOiJTYWRhbm91bWkiLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMTEgRWFzdCIsIndlc3RJZCI6MzAsIndlc3RTaGlrb25hIjoiS290b2VrbyIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSAxMCBXZXN0Iiwia2ltYXJpdGUiOiIiLCJ3aW5uZXJJZCI6MCwid2lubmVyRW4iOiIiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS04LTE1LTI2IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjgsImVhc3RJZCI6MTUsImVhc3RTaGlrb25hIjoiUnl1ZGVuIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDEwIEVhc3QiLCJ3ZXN0SWQiOjI2LCJ3ZXN0U2hpa29uYSI6Ik1pdGFrZXVtaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA5IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTktMzYtNzQiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6OSwiZWFzdElkIjozNiwiZWFzdFNoaWtvbmEiOiJNeW9naXJ5dSIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSA5IEVhc3QiLCJ3ZXN0SWQiOjc0LCJ3ZXN0U2hpa29uYSI6IkF0YW1pZnVqaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA4IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTEwLTE3LTUwIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjEwLCJlYXN0SWQiOjE3LCJlYXN0U2hpa29uYSI6IkVuZG8iLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgOCBFYXN0Iiwid2VzdElkIjo1MCwid2VzdFNoaWtvbmEiOiJLaW5ib3phbiIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA3IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTExLTUzLTM3IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjExLCJlYXN0SWQiOjUzLCJlYXN0U2hpa29uYSI6Ikhva3VzZWlobyIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSA3IEVhc3QiLCJ3ZXN0SWQiOjM3LCJ3ZXN0U2hpa29uYSI6IlRha2Fub3NobyIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA2IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTEyLTQ5LTM0IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjEyLCJlYXN0SWQiOjQ5LCJlYXN0U2hpa29uYSI6IlNob25hbm5vdW1pIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDYgRWFzdCIsIndlc3RJZCI6MzQsIndlc3RTaGlrb25hIjoiTWlkb3JpZnVqaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA1IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTEzLTEwLTE2IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjEzLCJlYXN0SWQiOjEwLCJlYXN0U2hpa29uYSI6Ik9ub3NobyIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSA1IEVhc3QiLCJ3ZXN0SWQiOjE2LCJ3ZXN0U2hpa29uYSI6Ik5pc2hpa2lnaSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA0IFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTE0LTIyLTU2IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjE0LCJlYXN0SWQiOjIyLCJlYXN0U2hpa29uYSI6IkFiaSIsImVhc3RSYW5rIjoiS29tdXN1YmkgMSBFYXN0Iiwid2VzdElkIjo1Niwid2VzdFNoaWtvbmEiOiJHb25veWFtYSIsIndlc3RSYW5rIjoiTWFlZ2FzaGlyYSA0IEVhc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn0seyJpZCI6IjIwMjMxMS0xLTE1LTIwLTIxIiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjE1LCJlYXN0SWQiOjIwLCJlYXN0U2hpa29uYSI6IktvdG9ub3dha2EiLCJlYXN0UmFuayI6IlNla2l3YWtlIDIgRWFzdCIsIndlc3RJZCI6MjEsIndlc3RTaGlrb25hIjoiVG9iaXphcnUiLCJ3ZXN0UmFuayI6Ik1hZWdhc2hpcmEgMyBXZXN0Iiwia2ltYXJpdGUiOiIiLCJ3aW5uZXJJZCI6MCwid2lubmVyRW4iOiIiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0xNi00NC0xMyIsImJhc2hvSWQiOiIyMDIzMTEiLCJkaXZpc2lvbiI6Ik1ha3V1Y2hpIiwiZGF5IjoxLCJtYXRjaE5vIjoxNiwiZWFzdElkIjo0NCwiZWFzdFNoaWtvbmEiOiJUYWtheWFzdSIsImVhc3RSYW5rIjoiTWFlZ2FzaGlyYSAzIEVhc3QiLCJ3ZXN0SWQiOjEzLCJ3ZXN0U2hpa29uYSI6Ildha2Ftb3RvaGFydSIsIndlc3RSYW5rIjoiU2VraXdha2UgMSBXZXN0Iiwia2ltYXJpdGUiOiIiLCJ3aW5uZXJJZCI6MCwid2lubmVyRW4iOiIiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0xNy05LTM4IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjE3LCJlYXN0SWQiOjksImVhc3RTaGlrb25hIjoiRGFpZWlzaG8iLCJlYXN0UmFuayI6IlNla2l3YWtlIDEgRWFzdCIsIndlc3RJZCI6MzgsIndlc3RTaGlrb25hIjoiTWVpc2VpIiwid2VzdFJhbmsiOiJNYWVnYXNoaXJhIDIgV2VzdCIsImtpbWFyaXRlIjoiIiwid2lubmVySWQiOjAsIndpbm5lckVuIjoiIiwid2lubmVySnAiOiIifSx7ImlkIjoiMjAyMzExLTEtMTgtMzMtMTkiLCJiYXNob0lkIjoiMjAyMzExIiwiZGl2aXNpb24iOiJNYWt1dWNoaSIsImRheSI6MSwibWF0Y2hObyI6MTgsImVhc3RJZCI6MzMsImVhc3RTaGlrb25hIjoiU2hvZGFpIiwiZWFzdFJhbmsiOiJNYWVnYXNoaXJhIDIgRWFzdCIsIndlc3RJZCI6MTksIndlc3RTaGlrb25hIjoiSG9zaG9yeXUiLCJ3ZXN0UmFuayI6Ik96ZWtpIDIgV2VzdCIsImtpbWFyaXRlIjoiIiwid2lubmVySWQiOjAsIndpbm5lckVuIjoiIiwid2lubmVySnAiOiIifSx7ImlkIjoiMjAyMzExLTEtMTktMjgtNyIsImJhc2hvSWQiOiIyMDIzMTEiLCJkaXZpc2lvbiI6Ik1ha3V1Y2hpIiwiZGF5IjoxLCJtYXRjaE5vIjoxOSwiZWFzdElkIjoyOCwiZWFzdFNoaWtvbmEiOiJVcmEiLCJlYXN0UmFuayI6Ik1hZWdhc2hpcmEgMSBXZXN0Iiwid2VzdElkIjo3LCJ3ZXN0U2hpa29uYSI6IktpcmlzaGltYSIsIndlc3RSYW5rIjoiT3pla2kgMSBXZXN0Iiwia2ltYXJpdGUiOiIiLCJ3aW5uZXJJZCI6MCwid2lubmVyRW4iOiIiLCJ3aW5uZXJKcCI6IiJ9LHsiaWQiOiIyMDIzMTEtMS0yMC0xLTI3IiwiYmFzaG9JZCI6IjIwMjMxMSIsImRpdmlzaW9uIjoiTWFrdXVjaGkiLCJkYXkiOjEsIm1hdGNoTm8iOjIwLCJlYXN0SWQiOjEsImVhc3RTaGlrb25hIjoiVGFrYWtlaXNobyIsImVhc3RSYW5rIjoiT3pla2kgMSBFYXN0Iiwid2VzdElkIjoyNywid2VzdFNoaWtvbmEiOiJIb2t1dG9mdWppIiwid2VzdFJhbmsiOiJLb211c3ViaSAxIFdlc3QiLCJraW1hcml0ZSI6IiIsIndpbm5lcklkIjowLCJ3aW5uZXJFbiI6IiIsIndpbm5lckpwIjoiIn1d"
  },
  "payload_decoded": [
    {
      "id": "202311-1-1-66-40",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 1,
      "eastId": 66,
      "eastShikona": "Kitanowaka",
      "eastRank": "Maegashira 17 East",
      "westId": 40,
      "westShikona": "Nishikifuji",
      "westRank": "Maegashira 16 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-2-55-71",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 2,
      "eastId": 55,
      "eastShikona": "Roga",
      "eastRank": "Maegashira 16 East",
      "westId": 71,
      "westShikona": "Churanoumi",
      "westRank": "Maegashira 15 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-3-54-11",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 3,
      "eastId": 54,
      "eastShikona": "Tohakuryu",
      "eastRank": "Maegashira 15 East",
      "westId": 11,
      "westShikona": "Ichiyamamoto",
      "westRank": "Maegashira 14 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-4-102-31",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 4,
      "eastId": 102,
      "eastShikona": "Tomokaze",
      "eastRank": "Maegashira 14 East",
      "westId": 31,
      "westShikona": "Tsurugisho",
      "westRank": "Maegashira 13 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-5-25-14",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 5,
      "eastId": 25,
      "eastShikona": "Takarafuji",
      "eastRank": "Maegashira 13 East",
      "westId": 14,
      "westShikona": "Tamawashi",
      "westRank": "Maegashira 12 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-6-41-24",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 6,
      "eastId": 41,
      "eastShikona": "Oho",
      "eastRank": "Maegashira 12 East",
      "westId": 24,
      "westShikona": "Hiradoumi",
      "westRank": "Maegashira 11 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-7-35-30",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 7,
      "eastId": 35,
      "eastShikona": "Sadanoumi",
      "eastRank": "Maegashira 11 East",
      "westId": 30,
      "westShikona": "Kotoeko",
      "westRank": "Maegashira 10 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-8-15-26",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 8,
      "eastId": 15,
      "eastShikona": "Ryuden",
      "eastRank": "Maegashira 10 East",
      "westId": 26,
      "westShikona": "Mitakeumi",
      "westRank": "Maegashira 9 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-9-36-74",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 9,
      "eastId": 36,
      "eastShikona": "Myogiryu",
      "eastRank": "Maegashira 9 East",
      "westId": 74,
      "westShikona": "Atamifuji",
      "westRank": "Maegashira 8 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-10-17-50",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 10,
      "eastId": 17,
      "eastShikona": "Endo",
      "eastRank": "Maegashira 8 East",
      "westId": 50,
      "westShikona": "Kinbozan",
      "westRank": "Maegashira 7 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-11-53-37",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 11,
      "eastId": 53,
      "eastShikona": "Hokuseiho",
      "eastRank": "Maegashira 7 East",
      "westId": 37,
      "westShikona": "Takanosho",
      "westRank": "Maegashira 6 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-12-49-34",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 12,
      "eastId": 49,
      "eastShikona": "Shonannoumi",
      "eastRank": "Maegashira 6 East",
      "westId": 34,
      "westShikona": "Midorifuji",
      "westRank": "Maegashira 5 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-13-10-16",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 13,
      "eastId": 10,
      "eastShikona": "Onosho",
      "eastRank": "Maegashira 5 East",
      "westId": 16,
      "westShikona": "Nishikigi",
      "westRank": "Maegashira 4 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-14-22-56",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 14,
      "eastId": 22,
      "eastShikona": "Abi",
      "eastRank": "Komusubi 1 East",
      "westId": 56,
      "westShikona": "Gonoyama",
      "westRank": "Maegashira 4 East",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-15-20-21",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 15,
      "eastId": 20,
      "eastShikona": "Kotonowaka",
      "eastRank": "Sekiwake 2 East",
      "westId": 21,
      "westShikona": "Tobizaru",
      "westRank": "Maegashira 3 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-16-44-13",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 16,
      "eastId": 44,
      "eastShikona": "Takayasu",
      "eastRank": "Maegashira 3 East",
      "westId": 13,
      "westShikona": "Wakamotoharu",
      "westRank": "Sekiwake 1 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-17-9-38",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 17,
      "eastId": 9,
      "eastShikona": "Daieisho",
      "eastRank": "Sekiwake 1 East",
      "westId": 38,
      "westShikona": "Meisei",
      "westRank": "Maegashira 2 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-18-33-19",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 18,
      "eastId": 33,
      "eastShikona": "Shodai",
      "eastRank": "Maegashira 2 East",
      "westId": 19,
      "westShikona": "Hoshoryu",
      "westRank": "Ozeki 2 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-19-28-7",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 19,
      "eastId": 28,
      "eastShikona": "Ura",
      "eastRank": "Maegashira 1 West",
      "westId": 7,
      "westShikona": "Kirishima",
      "westRank": "Ozeki 1 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    },
    {
      "id": "202311-1-20-1-27",
      "bashoId": "202311",
      "division": "Makuuchi",
      "day": 1,
      "matchNo": 20,
      "eastId": 1,
      "eastShikona": "Takakeisho",
      "eastRank": "Ozeki 1 East",
      "westId": 27,
      "westShikona": "Hokutofuji",
      "westRank": "Komusubi 1 West",
      "kimarite": "",
      "winnerId": 0,
      "winnerEn": "",
      "winnerJp": ""
    }
  ]
}

from utils.save_to_s3 import _save_to_s3
from utils.api_call import get_json

def insert_match(cursor, match_id, basho_id, division, day, match_number, east_id, east_shikona, east_rank, west_id, west_shikona, west_rank, winner_id, kimarite):
  """Insert a match using the provided match_id (string)."""
  cursor.execute(
    '''
    INSERT INTO matches (
      id, basho_id, division, day, match_number, east_rikishi_id, eastShikona, east_rank, west_rikishi_id, westShikona, west_rank, winner, kimarite
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    ''',
    # Use the provided match id (string) instead of concatenating integers which may overflow
    (match_id, basho_id, division, day, match_number, east_id, east_shikona, east_rank, west_id, west_shikona, west_rank, winner_id, kimarite)
  )
  
def insert_rikishi(cursor, rikishi_id, shikona, birthdate, retirement_date, current_rank, heya, shusshin, current_height, current_weight, debut, last_match, basho_count, absent_count, wins, losses, matches, yusho_count, sansho_count):
    cursor.execute(
        '''
        INSERT INTO rikishi (
            id, shikona, birthdate, retirement_date, current_rank, heya, shusshin, current_height, current_weight, debut, last_match, basho_count, absent_count, wins, losses, matches, yusho_count, sansho_count
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        ''',
        (rikishi_id, shikona, birthdate, retirement_date, current_rank, heya, shusshin, current_height, current_weight, debut, last_match, basho_count, absent_count, wins, losses, matches, yusho_count, sansho_count)
    )


def rikishi_exists(cursor, rikishi_id):
  if rikishi_id is None:
    return True
  cursor.execute('SELECT 1 FROM rikishi WHERE id = %s', (rikishi_id,))
  return cursor.fetchone() is not None


if webhook.get('type') == 'newMatches':
  try:
    conn = psycopg2.connect(
      dbname=os.getenv("DB_NAME"),
      user=os.getenv("DB_USERNAME"),
      password=os.getenv("DB_PASSWORD"),
      host=os.getenv("DB_HOST"),
      port=os.getenv("DB_PORT", 5432),
    )
    cur = conn.cursor()

    for match in webhook.get('payload_decoded', []):
      # validate and pull the canonical match id (string) from payload
      match_id = match.get('bashoId') + str(match.get('day')) + str(match.get('matchNo')) + str(match.get('eastId')) + str(match.get('westId'))
      match_id = int(match_id)
      print(f"Processing match ID: {match_id}")
      if not match_id:
        print(f"Skipping match with missing id: {match}")
        continue

      east_id = match.get('eastId')
      west_id = match.get('westId')
      winner_id = match.get('winnerId') if match.get('winnerId') != 0 else None
      


      if not rikishi_exists(cur, east_id):
        print(f"East rikishi ID {east_id} does not exist in rikishi table. Inserting placeholder rikishi.")
        match_date = f"{match['bashoId'][:4]}-{match['bashoId'][4:6]}-{match['bashoId'][6:8]}"

        rikishi = get_json(f"/rikishi/{east_id}")
        _save_to_s3(rikishi, S3_PREFIX + "rikishis", f"rikishi_{east_id}")
      
        rikishi_id = rikishi['id']
        shikona_en = rikishi.get('shikonaEn', '')
        shikona_jp = rikishi.get('shikonaJp', '')
        shikona = f"{shikona_en} ({shikona_jp})" if shikona_jp else shikona_en
        birthdate = rikishi.get('birthDate', None)
        if birthdate:
            birthdate = birthdate[:10]
        current_rank = rikishi.get('currentRank')
        heya = rikishi.get('heya')
        shusshin = rikishi.get('shusshin')
        current_height = rikishi.get('height')
        current_weight = rikishi.get('weight')
        debut = rikishi.get('debut')
        retirement_date = rikishi.get('intai', None)
        if debut and len(debut) == 6:
            debut = f"{debut[:4]}-{debut[4:6]}-01"
        else:
            debut = None
        last_match = rikishi.get('updatedAt', None)
        if last_match:
            last_match = last_match[:10]
            
        basho_count = 1
        absent_count = 0
        wins = 0
        losses = 0
        matches = 0
        yusho_count = 0
        sansho_count = 0
        
        insert_rikishi(cur, rikishi_id, shikona, birthdate, retirement_date, current_rank, heya, shusshin, current_height, current_weight, debut, last_match, basho_count, absent_count, wins, losses, matches, yusho_count, sansho_count)


      if not rikishi_exists(cur, west_id):
        print(f"West rikishi ID {west_id} does not exist in rikishi table. Inserting placeholder rikishi.")
        match_date = f"{match['bashoId'][:4]}-{match['bashoId'][4:6]}-{match['bashoId'][6:8]}"
        
        
        rikishi = get_json(f"/rikishi/{west_id}")
        _save_to_s3(rikishi, S3_PREFIX + "rikishis", f"rikishi_{west_id}")
      
        rikishi_id = rikishi['id']
        shikona_en = rikishi.get('shikonaEn', '')
        shikona_jp = rikishi.get('shikonaJp', '')
        shikona = f"{shikona_en} ({shikona_jp})" if shikona_jp else shikona_en
        birthdate = rikishi.get('birthDate', None)
        if birthdate:
            birthdate = birthdate[:10]
        current_rank = rikishi.get('currentRank')
        heya = rikishi.get('heya')
        shusshin = rikishi.get('shusshin')
        current_height = rikishi.get('height')
        current_weight = rikishi.get('weight')
        debut = rikishi.get('debut')
        retirement_date = rikishi.get('intai', None)
        if debut and len(debut) == 6:
            debut = f"{debut[:4]}-{debut[4:6]}-01"
        else:
            debut = None
        last_match = rikishi.get('updatedAt', None)
        if last_match:
            last_match = last_match[:10]
            
        basho_count = 1
        absent_count = 0
        wins = 0
        losses = 0
        matches = 0
        yusho_count = 0
        sansho_count = 0
        
        insert_rikishi(cur, rikishi_id, shikona, birthdate, retirement_date, current_rank, heya, shusshin, current_height, current_weight, debut, last_match, basho_count, absent_count, wins, losses, matches, yusho_count, sansho_count)

      try:
        insert_match(
          cur,
          match_id,
          match.get('bashoId'),
          match.get('division'),
          match.get('day'),
          match.get('matchNo'),
          east_id,
          match.get('eastShikona'),
          match.get('eastRank'),
          west_id,
          match.get('westShikona'),
          match.get('westRank'),
          winner_id,
          match.get('kimarite'),
        )
        print(f"match {match_id} inserted or already exists")
      except Exception as ie:
        # Log the individual match error but continue processing others
        print(f"Failed to insert match {match_id}:", ie)

    conn.commit()
    cur.close()
    conn.close()
    print("New matches recorded in database.")
  except Exception as e:
    print("Database error:", e)

