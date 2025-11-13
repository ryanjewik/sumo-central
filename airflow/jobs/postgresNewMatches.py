from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
import os, sys, json, time
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from utils.api_call import get_json

load_dotenv()

S3_BUCKET = os.getenv("S3_BUCKET")
S3_REGION = os.getenv("AWS_REGION")
S3_PREFIX = os.getenv("S3_PREFIX", "sumo-api-calls/")



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


def basho_exists(cursor, basho_id):
  if not basho_id:
    return True
  cursor.execute('SELECT 1 FROM basho WHERE id = %s', (basho_id,))
  return cursor.fetchone() is not None


def insert_basho(cursor, basho_id, name, location, start_date, end_date):
  # Normalize basho id to int when possible (basho.id is an integer in the DB)
  try:
    bid = int(basho_id)
  except Exception:
    bid = basho_id

  # Normalize dates to YYYY-MM-DD (Postgres date fields expect date-like values)
  def _norm_date(d):
    if not d:
      return None
    s = str(d)
    # API may return ISO timestamps like 2025-11-09T00:00:00Z; keep only date portion
    if len(s) >= 10:
      return s[:10]
    return s

  s_date = _norm_date(start_date)
  e_date = _norm_date(end_date)

  cursor.execute(
    '''
    INSERT INTO basho (
      id, name, location, start_date, end_date, makuuchi_yusho, juryo_yusho, makushita_yusho, jonidan_yusho, sandanme_yusho
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO NOTHING;
    ''',
    (bid, name, location, s_date, e_date, None, None, None, None, None)
  )


def process_new_matches(webhook: dict):
  if not webhook or webhook.get('type') != 'newMatches':
    return

  # Prefer Airflow hooks when running inside an Airflow worker; fall back to
  # environment variables (and a direct psycopg2 connect) if not available.
  pg_conn_id = os.getenv("POSTGRES_CONN_ID", "postgres_default")
  s3_conn_id = os.getenv("S3_CONN_ID", "aws_default")

  try:
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    conn = pg_hook.get_conn()
    cur = conn.cursor()
  except Exception:
    # If PostgresHook isn't available or fails, surface a helpful error
    raise
  # initialize S3 hook for saving rikishi payloads
  try:
    s3_hook = S3Hook(aws_conn_id=s3_conn_id)
  except Exception:
    s3_hook = None
    

  for match in webhook.get('payload_decoded', []):
      # validate and pull the canonical match id (string) from payload
      match_id = match.get('bashoId') + str(match.get('day')) + str(match.get('matchNo')) + str(match.get('eastId')) + str(match.get('westId'))
      match_id = int(match_id)
      print(f"Processing match ID: {match_id}")
      if not match_id:
        print(f"Skipping match with missing id: {match}")
        continue

      # Ensure the referenced basho exists in the database. If not, try to fetch
      # the minimal basho payload from the API and insert it (date, start/end,
      # optional location). This mirrors the behavior in postgresNewBasho.
      basho_id = match.get('bashoId')
      if not basho_exists(cur, basho_id):
        print(f"Basho {basho_id} not found in DB; fetching from API and inserting minimal row.")
        try:
          basho = get_json(f"/basho/{basho_id}")
        except Exception as exc:
          print(f"Failed to fetch basho {basho_id} from API: {exc}")
          basho = None

        if basho:
          date = basho.get('date') or basho_id
          start_date = basho.get('startDate')
          end_date = basho.get('endDate')
          location = basho.get('location') if 'location' in basho else None
        else:
          # Use whatever minimal info we have from the match payload
          date = basho_id
          start_date = None
          end_date = None
          location = None

        try:
          insert_basho(cur, date, f"Basho {date}", location, start_date, end_date)
          print(f"Inserted basho {date} into DB (or it already existed).")
        except Exception as exc:
          print(f"Failed to insert basho {basho_id}: {exc}")

      east_id = match.get('eastId')
      west_id = match.get('westId')
      winner_id = match.get('winnerId') if match.get('winnerId') != 0 else None

      if not rikishi_exists(cur, east_id):
        print(f"East rikishi ID {east_id} does not exist in rikishi table. Inserting placeholder rikishi.")
        match_date = f"{match['bashoId'][:4]}-{match['bashoId'][4:6]}-{match['bashoId'][6:8]}"

        rikishi = get_json(f"/rikishi/{east_id}")
        # persist rikishi snapshot to S3 (if hook available)
        try:
          if s3_hook and S3_BUCKET:
            s3_key = f"{S3_PREFIX}rikishis/rikishi_{east_id}.json"
            s3_hook.load_string(json.dumps(rikishi), key=s3_key, bucket_name=S3_BUCKET, replace=True)
        except Exception as exc:
          print(f"Failed to save rikishi {east_id} to S3: {exc}")

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
        try:
          if s3_hook and S3_BUCKET:
            s3_key = f"{S3_PREFIX}rikishis/rikishi_{west_id}.json"
            s3_hook.load_string(json.dumps(rikishi), key=s3_key, bucket_name=S3_BUCKET, replace=True)
        except Exception as exc:
          print(f"Failed to save rikishi {west_id} to S3: {exc}")

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

