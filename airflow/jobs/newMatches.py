from concurrent.futures import ThreadPoolExecutor
import psycopg2
from dotenv import load_dotenv
import os, sys, json, time

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
S3_REGION = os.getenv("AWS_REGION")
S3_PREFIX = os.getenv("S3_PREFIX", "sumo-api-calls/")


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


def process_new_matches(webhook: dict):
  """Process a newMatches webhook: insert matches and create placeholder rikishi where needed."""
  if not webhook or webhook.get('type') != 'newMatches':
    return

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


if __name__ == "__main__":
  sample_webhook = {
    "received_at": 1756357623,
    "type": "newMatches",
    "payload_decoded": [
      {"id": "202311-1-1-66-40", "bashoId": "202311", "division": "Makuuchi", "day": 1, "matchNo": 1, "eastId": 66, "westId": 40},
      {"id": "202311-1-2-55-71", "bashoId": "202311", "division": "Makuuchi", "day": 1, "matchNo": 2, "eastId": 55, "westId": 71}
    ]
  }
  process_new_matches(sample_webhook)

