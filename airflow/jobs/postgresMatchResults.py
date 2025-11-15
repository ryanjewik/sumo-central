import os, json
import psycopg2
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from utils.api_call import get_json

load_dotenv()

S3_PREFIX = os.getenv("S3_PREFIX", "sumo-api-calls/")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_REGION = os.getenv("AWS_REGION")


try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except Exception:
    PostgresHook = None

try:
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
except Exception:
    S3Hook = None


def insert_match(cursor, match_id, basho_id, division, day, match_number, east_id, east_shikona, east_rank, west_id, west_shikona, west_rank, winner_id, kimarite):
    # Use an upsert so incoming matchResults update existing rows (e.g., add winner/kimarite)
    # while still inserting when the match doesn't exist yet.
    cursor.execute(
        '''
        INSERT INTO matches (
            id, basho_id, division, day, match_number, east_rikishi_id, eastShikona, east_rank, west_rikishi_id, westShikona, west_rank, winner, kimarite
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            basho_id = COALESCE(EXCLUDED.basho_id, matches.basho_id),
            division = COALESCE(EXCLUDED.division, matches.division),
            day = COALESCE(EXCLUDED.day, matches.day),
            match_number = COALESCE(EXCLUDED.match_number, matches.match_number),
            east_rikishi_id = COALESCE(EXCLUDED.east_rikishi_id, matches.east_rikishi_id),
            eastShikona = COALESCE(EXCLUDED.eastShikona, matches.eastShikona),
            east_rank = COALESCE(EXCLUDED.east_rank, matches.east_rank),
            west_rikishi_id = COALESCE(EXCLUDED.west_rikishi_id, matches.west_rikishi_id),
            westShikona = COALESCE(EXCLUDED.westShikona, matches.westShikona),
            west_rank = COALESCE(EXCLUDED.west_rank, matches.west_rank),
            winner = COALESCE(EXCLUDED.winner, matches.winner),
            kimarite = COALESCE(EXCLUDED.kimarite, matches.kimarite);
        ''',
        (match_id, basho_id, division, day, match_number, east_id, east_shikona, east_rank, west_id, west_shikona, west_rank, winner_id, kimarite)
    )


def process_match_results(webhook: dict):
    if not webhook or webhook.get('type') != 'matchResults':
        return

    rikishi_list = []
    processed_match_ids = []

    conn = None
    cur = None
    try:
        pg_conn_id = os.getenv("POSTGRES_CONN_ID", "postgres_default")
        # Prefer Airflow PostgresHook when available
        if PostgresHook:
            try:
                pg = PostgresHook(postgres_conn_id=pg_conn_id)
                conn = pg.get_conn()
            except Exception as e:
                print(f"PostgresHook connection failed: {e}; will try psycopg2 fallback")

        # Fallback to psycopg2 if PostgresHook not available or failed
        if conn is None:
            try:
                # Try DATABASE_URL / POSTGRES_URL first
                pg_urls = os.getenv('DATABASE_URL') or os.getenv('POSTGRES_URL')
                if pg_urls:
                    conn = psycopg2.connect(pg_urls)
                else:
                    pg_host = os.getenv('POSTGRES_HOST')
                    pg_port = os.getenv('POSTGRES_PORT')
                    pg_db = os.getenv('POSTGRES_DB') or os.getenv('POSTGRES_DATABASE')
                    pg_user = os.getenv('POSTGRES_USER')
                    pg_pass = os.getenv('POSTGRES_PASSWORD')
                    conn_args = {}
                    if pg_host:
                        conn_args['host'] = pg_host
                    if pg_port:
                        conn_args['port'] = pg_port
                    if pg_db:
                        conn_args['dbname'] = pg_db
                    if pg_user:
                        conn_args['user'] = pg_user
                    if pg_pass:
                        conn_args['password'] = pg_pass
                    conn = psycopg2.connect(**conn_args)
            except Exception as e:
                print(f"Failed to establish Postgres connection: {e}")
                raise

        cur = conn.cursor()

        for match in webhook.get('payload_decoded', []):
            east_id = match.get('eastId')
            west_id = match.get('westId')
            if east_id and east_id not in rikishi_list:
                rikishi_list.append(east_id)
            if west_id and west_id not in rikishi_list:
                rikishi_list.append(west_id)

            # stable match id
            match_id = match.get('bashoId') + str(match.get('day')) + str(match.get('matchNo')) + str(east_id) + str(west_id)
            try:
                match_id_int = int(match_id)
            except Exception:
                match_id_int = match_id
            # remember processed matches for optional Redis cleanup later
            processed_match_ids.append(match_id_int)

            insert_match(
                cur,
                match_id_int,
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
                match.get('winnerId'),
                match.get('kimarite'),
            )

            # Evaluate user predictions for this match and update match_predictions.is_correct
            try:
                # Fetch predictions for this match
                cur.execute("SELECT user_id, predicted_winner, is_correct FROM match_predictions WHERE match_id = %s", (match_id_int,))
                preds = cur.fetchall()
                for row in preds:
                    try:
                        user_id = row[0]
                        predicted_winner = row[1]
                        is_correct_existing = row[2]
                        # If already resolved (is_correct not null), skip to avoid double-counting
                        if is_correct_existing is not None:
                            continue
                        was_correct = (predicted_winner == match.get('winnerId'))
                        # Update the prediction row
                        cur.execute("UPDATE match_predictions SET is_correct = %s WHERE user_id = %s AND match_id = %s", (was_correct, user_id, match_id_int))

                        # Update user's aggregate counters
                        # Increment num_predictions and either correct_predictions or false_predictions
                        cur.execute(
                            """
                            UPDATE users SET
                              num_predictions = COALESCE(num_predictions,0) + 1,
                              correct_predictions = COALESCE(correct_predictions,0) + %s,
                              false_predictions = COALESCE(false_predictions,0) + %s
                            WHERE id = %s
                            """,
                            (1 if was_correct else 0, 0 if was_correct else 1, user_id)
                        )

                        # Recompute predictions_ratio for this user
                        cur.execute(
                            "UPDATE users SET predictions_ratio = CASE WHEN COALESCE(num_predictions,0) > 0 THEN (COALESCE(correct_predictions,0)::float / num_predictions) ELSE 0 END WHERE id = %s",
                            (user_id,)
                        )
                    except Exception as e:
                        print(f"Failed to process prediction row {row}: {e}")
            except Exception as e:
                print(f"Failed to fetch/update match_predictions for match {match_id_int}: {e}")

            winner = match.get('winnerId')
            if winner and winner == east_id:
                cur.execute(
                    """
                    UPDATE rikishi
                    SET matches = matches + 1,
                        wins = wins + 1
                    WHERE id = %s
                    """,
                    (east_id,)
                )
                cur.execute(
                    """
                    UPDATE rikishi
                    SET matches = matches + 1,
                        losses = losses + 1
                    WHERE id = %s
                    """,
                    (west_id,)
                )
            else:
                cur.execute(
                    """
                    UPDATE rikishi
                    SET matches = matches + 1,
                        wins = wins + 1
                    WHERE id = %s
                    """,
                    (west_id,)
                )
                cur.execute(
                    """
                    UPDATE rikishi
                    SET matches = matches + 1,
                        losses = losses + 1
                    WHERE id = %s
                    """,
                    (east_id,)
                )

        conn.commit()

        # Clear ephemeral Redis keys for finalized matches so live vote state
        # doesn't persist after a match is decided. Run unconditionally; if the
        # redis client isn't available or connection fails we log and continue.
        if processed_match_ids:
            try:
                try:
                    import redis as _redis
                except Exception:
                    _redis = None
                if _redis is None:
                    print('Redis cleanup requested but redis python client is not available; skipping')
                else:
                    # Build a sensible Redis URL. Prefer REDIS_URL, then REDIS_HOST; if only
                    # REDIS_HOST is provided and doesn't contain a scheme, construct a
                    # redis:// URL using REDIS_PORT/REDIS_DB if available. Finally, fall
                    # back to the compose service name 'redis' rather than localhost so
                    # the Airflow container can reach the Redis container when using
                    # docker compose networking.
                    redis_url = os.getenv('REDIS_URL')
                    redis_host = os.getenv('REDIS_HOST')
                    if not redis_url:
                        if redis_host:
                            # If REDIS_HOST contains a scheme, use it as-is, otherwise
                            # build a redis:// URL
                            if '://' in redis_host:
                                redis_url = redis_host
                            else:
                                redis_port = os.getenv('REDIS_PORT', '6379')
                                redis_db = os.getenv('REDIS_DB', '0')
                                redis_url = f"redis://{redis_host}:{redis_port}/{redis_db}"
                        else:
                            # Default to the compose service name 'redis'
                            redis_url = 'redis://redis:6379/0'

                    try:
                        print(f"Redis cleanup: attempting to connect to redis at {redis_url}")
                        rcli = _redis.from_url(redis_url, decode_responses=True)
                        for mid in processed_match_ids:
                            votes_key = f"match_votes:{mid}"
                            users_key = f"match_users:{mid}"
                            try:
                                deleted = rcli.delete(votes_key, users_key)
                                print(f"Redis cleanup: deleted {deleted} keys for match {mid} -> {votes_key}, {users_key}")
                            except Exception as e:
                                print(f"Redis cleanup: failed to delete keys for match {mid}: {e}")
                    except Exception as e:
                        print(f"Redis cleanup: failed to connect to redis at {redis_url}: {e}")
            except Exception as e:
                print(f"Redis cleanup step failed unexpectedly: {e}")
        # initialize S3 hook and fetch per-rikishi matches and save to S3
        s3_conn_id = os.getenv("S3_CONN_ID", "aws_default")
        try:
            s3_hook = S3Hook(aws_conn_id=s3_conn_id) if S3Hook else None
        except Exception:
            s3_hook = None

        # fetch per-rikishi matches and save to S3
        _session = requests.Session()
        _retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=0.5,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
        )
        _adapter = HTTPAdapter(max_retries=_retry)
        _session.mount("http://", _adapter)
        _session.mount("https://", _adapter)

        for rikishi in rikishi_list:
            matches = get_json(f"/rikishi/{rikishi}/matches")
            try:
                if s3_hook and S3_BUCKET:
                    s3_key = f"{S3_PREFIX}rikishi_matches/rikishi_{rikishi}.json"
                    try:
                        s3_hook.load_string(json.dumps(matches), key=s3_key, bucket_name=S3_BUCKET, replace=True)
                    except Exception as exc:
                        errstr = str(exc)
                        print(f"S3Hook.load_string failed for rikishi {rikishi}: {errstr}")
                        # Signature mismatch is commonly caused by incorrect credentials, region
                        # or connection config in the Airflow connection. Provide a helpful
                        # diagnostic and attempt a boto3 fallback using configured env vars.
                        if 'SignatureDoesNotMatch' in errstr or 'Signature' in errstr:
                            print("S3 signature error detected. Check AWS credentials and region on the Airflow connection (aws_default or S3_CONN_ID). Trying boto3 fallback...")
                        try:
                            import boto3
                            from botocore.client import Config
                            aws_region = S3_REGION or os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION')
                            cfg = Config(signature_version='s3v4', region_name=aws_region) if aws_region else Config(signature_version='s3v4')
                            boto_s3 = boto3.client('s3', config=cfg)
                            boto_s3.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(matches).encode('utf-8'))
                            print(f"Successfully saved rikishi {rikishi} matches via boto3 fallback to {S3_BUCKET}/{s3_key}")
                        except Exception as bexc:
                            print(f"boto3 fallback failed for rikishi {rikishi}: {bexc}")
                            # fallback to local helper if present
                            try:
                                from utils.save_to_s3 import _save_to_s3
                                _save_to_s3(matches, S3_PREFIX + "rikishi_matches", f"rikishi_{rikishi}")
                            except Exception:
                                print(f"No S3 hook or local saver available for rikishi {rikishi}")
                else:
                    # fallback to local helper if present
                    try:
                        from utils.save_to_s3 import _save_to_s3
                        _save_to_s3(matches, S3_PREFIX + "rikishi_matches", f"rikishi_{rikishi}")
                    except Exception:
                        print(f"No S3 hook or local saver available for rikishi {rikishi}")
            except Exception as exc:
                print(f"Failed to save matches for rikishi {rikishi}: {exc}")

    except Exception as e:
        print("Database error:", e)


if __name__ == "__main__":
    sample_webhook = {
        "received_at": 1756357624,
        "type": "matchResults",
        "payload_decoded": [
            {"id": "202311-1-1-66-40", "bashoId": "202311", "division": "Makuuchi", "day": 1, "matchNo": 1, "eastId": 66, "westId": 40, "kimarite": "yoritaoshi", "winnerId": 66},
            {"id": "202311-1-2-55-71", "bashoId": "202311", "division": "Makuuchi", "day": 1, "matchNo": 2, "eastId": 55, "westId": 71, "kimarite": "yorikiri", "winnerId": 71}
        ]
    }
    process_match_results(sample_webhook)


