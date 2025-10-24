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
    cursor.execute(
        '''
        INSERT INTO matches (
            id, basho_id, division, day, match_number, east_rikishi_id, eastShikona, east_rank, west_rikishi_id, westShikona, west_rank, winner, kimarite
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING;
        ''',
        (match_id, basho_id, division, day, match_number, east_id, east_shikona, east_rank, west_id, west_shikona, west_rank, winner_id, kimarite)
    )


def process_match_results(webhook: dict):
    if not webhook or webhook.get('type') != 'matchResults':
        return

    rikishi_list = []

    try:
        pg_conn_id = os.getenv("POSTGRES_CONN_ID", "postgres_default")
        if PostgresHook:
            pg = PostgresHook(postgres_conn_id=pg_conn_id)
            conn = pg.get_conn()
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
        cur.close()
        conn.close()

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
                    s3_hook.load_string(json.dumps(matches), key=s3_key, bucket_name=S3_BUCKET, replace=True)
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


