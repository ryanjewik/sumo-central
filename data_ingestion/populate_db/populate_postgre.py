import psycopg2
from dotenv import load_dotenv
load_dotenv()
import os, sys, json, time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

#user database connection with retry mechanism
def connect_to_database(max_retries=30, delay=2):
    """
    Attempt to connect to the database with retry logic
    """
    for attempt in range(max_retries):
        try:
            #print(f"Attempting to connect to database (attempt {attempt + 1}/{max_retries}):")

            
            conn = psycopg2.connect(
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            #print("✅ Database connection successful!")
            return conn
        except Exception as e:
            print(f"❌ Error connecting to the database (attempt {attempt + 1}): {e}")
            if attempt < max_retries - 1:
                print(f"⏳ Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                print(f"💀 Failed to connect to database after {max_retries} attempts")
                raise e
            
try:
    conn = connect_to_database()
except Exception as e:
    print("❌ Failed to establish database connection:", e)
    conn = None
    


if conn: # drop and create tables------------------------------------------------------
    try:
        cursor = conn.cursor()
        # Drop tables in order of dependencies
        drop_table_queries = [
            "DROP TABLE IF EXISTS match_predictions CASCADE;",
            "DROP TABLE IF EXISTS matches CASCADE;",
            "DROP TABLE IF EXISTS special_prizes CASCADE;",
            "DROP TABLE IF EXISTS users CASCADE;",
            "DROP TABLE IF EXISTS rikishi_rank_history CASCADE;",
            "DROP TABLE IF EXISTS rikishi_measurements_history CASCADE;",
            "DROP TABLE IF EXISTS rikishi_shikona_changes CASCADE;",
            "DROP TABLE IF EXISTS basho CASCADE;",
            "DROP TABLE IF EXISTS rikishi CASCADE;"
        ]
        for query in drop_table_queries:
            cursor.execute(query)
        conn.commit()

        # Enable uuid-ossp extension for UUID generation (if not already enabled)
        cursor.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
        conn.commit()

        # Create rikishi table first (id from sumo-api, not SERIAL)
        create_rikishi_table_query = '''
        CREATE TABLE IF NOT EXISTS rikishi (
            id INTEGER PRIMARY KEY,
            shikona VARCHAR(255) NOT NULL,
            birthdate DATE,
            retirement_date DATE,
            current_rank VARCHAR(255),
            heya VARCHAR(255),
            shusshin VARCHAR(255),
            -- Image / media metadata added so update_rikishi_image() can write S3/Commons fields
            image_url TEXT,
            commons_source_url TEXT,
            license VARCHAR(255),
            license_url TEXT,
            attribution_html TEXT,
            credit_html TEXT,
            s3_key TEXT,
            s3_url TEXT,
            -- image dimensions and mime type
            width INT,
            height INT,
            mime VARCHAR(255),
            current_height INT,
            current_weight INT,
            debut DATE,
            last_match DATE,
            basho_count INT,
            absent_count INT,
            wins INT,
            losses INT,
            matches INT,
            yusho_count INT,
            sansho_count INT
        );
        '''
        cursor.execute(create_rikishi_table_query)
        conn.commit()
        print("✅ Rikishi table ensured in database.")

        # Create basho table (mock)
        create_basho_table_query = '''
        CREATE TABLE IF NOT EXISTS basho (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            location VARCHAR(255),
            start_date DATE,
            end_date DATE,
            makuuchi_yusho INT REFERENCES rikishi(id) ON DELETE SET NULL ON UPDATE CASCADE,
            juryo_yusho INT REFERENCES rikishi(id) ON DELETE SET NULL ON UPDATE CASCADE,
            sandanme_yusho INT REFERENCES rikishi(id) ON DELETE SET NULL ON UPDATE CASCADE,
            makushita_yusho INT REFERENCES rikishi(id) ON DELETE SET NULL ON UPDATE CASCADE,
            jonidan_yusho INT REFERENCES rikishi(id) ON DELETE SET NULL ON UPDATE CASCADE,
            jonokuchi_yusho INT REFERENCES rikishi(id) ON DELETE SET NULL ON UPDATE CASCADE
        );
        '''
        cursor.execute(create_basho_table_query)
        print("✅ Basho table ensured in database.")

        # special_prizes table
        create_special_prizes_table_query = '''
        CREATE TABLE IF NOT EXISTS special_prizes (
            id SERIAL PRIMARY KEY,
            basho_id INT REFERENCES basho(id),
            rikishi_id INT REFERENCES rikishi(id),
            prize_name VARCHAR(255)
        );
        '''
        cursor.execute(create_special_prizes_table_query)
        conn.commit()
        print("✅ Special prizes table ensured in database.")

        # Now create users table (UUID for id)
        create_users_table_query = '''
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            username VARCHAR(100) UNIQUE NOT NULL,
            email VARCHAR(100) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            password VARCHAR(100) NOT NULL,
            predictions_ratio FLOAT DEFAULT 0.0,
            favorite_rikishi INT,
            CONSTRAINT fk_favorite_rikishi FOREIGN KEY (favorite_rikishi) REFERENCES rikishi (id),
            num_posts INT DEFAULT 0,
            num_predictions INT DEFAULT 0,
            correct_predictions INT DEFAULT 0,
            false_predictions INT DEFAULT 0,
            country VARCHAR(100) DEFAULT ''
        );
        '''
        cursor.execute(create_users_table_query)
        conn.commit()
        print("✅ User table ensured in database.")

        # Create matches table (mock)
        create_matches_table_query = '''
        CREATE TABLE IF NOT EXISTS matches (
            id BIGINT PRIMARY KEY,
            basho_id INT REFERENCES basho(id),
            east_rikishi_id INT REFERENCES rikishi(id),
            west_rikishi_id INT REFERENCES rikishi(id),
            east_rank VARCHAR(255),
            west_rank VARCHAR(255),
            eastShikona VARCHAR(255),
            westShikona VARCHAR(255),
            winner INT REFERENCES rikishi(id),
            kimarite VARCHAR(255),
            day INT,
            match_number INT,
            division VARCHAR(255)
        );
        '''
        cursor.execute(create_matches_table_query)
        conn.commit()
        print("✅ Matches table ensured in database.")

        # Create match_predictions table (mock)
        create_match_predictions_table_query = '''
        CREATE TABLE IF NOT EXISTS match_predictions (
            id SERIAL PRIMARY KEY,
            user_id UUID REFERENCES users(id),
            match_id BIGINT REFERENCES matches(id),
            predicted_winner INT REFERENCES rikishi(id),
            prediction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_correct BOOLEAN
        );
        ALTER TABLE match_predictions
        ADD CONSTRAINT uniq_user_match
        UNIQUE (user_id, match_id);
        '''
        cursor.execute(create_match_predictions_table_query)
        conn.commit()
        print("✅ Match predictions table ensured in database.")

        # Create rikishi_rank_history table
        create_rank_history_table_query = '''
        CREATE TABLE IF NOT EXISTS rikishi_rank_history (
            id SERIAL PRIMARY KEY,
            rikishi_id INT REFERENCES rikishi(id),
            rank_name VARCHAR(255) NOT NULL,
            rank_value INT NOT NULL,
            rank_date DATE NOT NULL,
            basho_id INT REFERENCES basho(id)
        );
        '''
        cursor.execute(create_rank_history_table_query)
        conn.commit()
        print("✅ Rikishi rank history table ensured in database.")

        # Create rikishi_measurements_history table
        create_measurements_history_table_query = '''
        CREATE TABLE IF NOT EXISTS rikishi_measurements_history (
            id SERIAL PRIMARY KEY,
            rikishi_id INT REFERENCES rikishi(id),
            height_cm INT,
            weight_kg INT,
            measurement_date DATE,
            basho_id INT REFERENCES basho(id)
        );
        '''
        cursor.execute(create_measurements_history_table_query)
        conn.commit()
        print("✅ Rikishi measurements history table ensured in database.")

        # Create rikishi_shikona_changes table
        create_shikona_changes_table_query = '''
        CREATE TABLE IF NOT EXISTS rikishi_shikona_changes (
            id SERIAL PRIMARY KEY,
            rikishi_id INT REFERENCES rikishi(id),
            shikona VARCHAR(255) NOT NULL,
            change_date DATE,
            basho_id INT REFERENCES basho(id)
        );
        '''
        cursor.execute(create_shikona_changes_table_query)
        conn.commit()
        print("✅ Rikishi shikona changes table ensured in database.")
        
        
        create_refresh_token_table_query = '''
        CREATE TABLE IF NOT EXISTS refresh_tokens (
        token UUID PRIMARY KEY,
        user_id UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        expires_at TIMESTAMPTZ NOT NULL,
        revoked BOOLEAN DEFAULT FALSE
        );

        CREATE INDEX IF NOT EXISTS idx_refresh_tokens_user_id ON refresh_tokens(user_id);
        '''
        cursor.execute(create_refresh_token_table_query)
        conn.commit()
        print("✅ Refresh tokens table ensured in database.")
        
        
    except Exception as e:
        print("❌ Error creating tables:", e)

# --- Insert function for basho table ---
def insert_basho(cursor, basho_id, name, location, start_date, end_date, yusho_dict):
    cursor.execute(
        '''
        INSERT INTO basho (
            id, name, location, start_date, end_date,
            makuuchi_yusho, juryo_yusho, makushita_yusho, sandanme_yusho, jonidan_yusho, jonokuchi_yusho
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            location = EXCLUDED.location,
            start_date = EXCLUDED.start_date,
            end_date = EXCLUDED.end_date,
            makuuchi_yusho  = COALESCE(EXCLUDED.makuuchi_yusho,  basho.makuuchi_yusho),
            juryo_yusho     = COALESCE(EXCLUDED.juryo_yusho,     basho.juryo_yusho),
            makushita_yusho = COALESCE(EXCLUDED.makushita_yusho, basho.makushita_yusho),
            sandanme_yusho  = COALESCE(EXCLUDED.sandanme_yusho,  basho.sandanme_yusho),
            jonidan_yusho   = COALESCE(EXCLUDED.jonidan_yusho,   basho.jonidan_yusho),
            jonokuchi_yusho = COALESCE(EXCLUDED.jonokuchi_yusho, basho.jonokuchi_yusho);
        ''',
        (
            basho_id,
            name,
            location,
            start_date,
            end_date,
            yusho_dict.get('Makuuchi'),
            yusho_dict.get('Juryo'),
            yusho_dict.get('Makushita'),
            yusho_dict.get('Sandanme'),
            yusho_dict.get('Jonidan'),
            yusho_dict.get('Jonokuchi')
        )
    )


# --- Insert function for special_prizes table ---
def insert_special_prize(cursor, basho_id, rikishi_id, prize_name):
    cursor.execute(
        '''
        INSERT INTO special_prizes (basho_id, rikishi_id, prize_name)
        VALUES (%s, %s, %s)
        ON CONFLICT DO NOTHING;
        ''',
        (basho_id, rikishi_id, prize_name)
    )

# --- Process basho JSON files ---
def process_basho_json(directory):
    def rikishi_exists(cursor, rikishi_id):
        cursor.execute('SELECT 1 FROM rikishi WHERE id = %s', (rikishi_id,))
        return cursor.fetchone() is not None

    # canonical division names keyed by lowercase
    DIV_MAP = {
        'makuuchi': 'Makuuchi',
        'juryo': 'Juryo',
        'makushita': 'Makushita',
        'sandanme': 'Sandanme',
        'jonidan': 'Jonidan',
        'jonokuchi': 'Jonokuchi',
    }

    def process_file(json_path):
        import re, json, os
        conn = connect_to_database()
        cursor = conn.cursor()
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        match = re.search(r'basho_(\d+)\.json$', os.path.basename(json_path))
        if not match:
            print(f"⚠️ Could not extract basho_id from filename: {json_path}")
            cursor.close(); conn.close(); return
        basho_id = int(match.group(1))

        name = (data.get('name') or '').strip() or f"Basho {basho_id}"
        location = data.get('location')
        start_date = data.get('startDate', '')[:10] if data.get('startDate') else None
        end_date = data.get('endDate', '')[:10] if data.get('endDate') else None

        yusho_dict = {}
        for y in data.get('yusho', []):
            raw_type = (y.get('type') or '').strip().lower()
            canon = DIV_MAP.get(raw_type)
            if not canon:
                print(f"⚠️ Unknown division type '{y.get('type')}' in {json_path}")
                continue
            rikishi_id = y.get('rikishiId')
            if rikishi_id and rikishi_exists(cursor, rikishi_id):
                yusho_dict[canon] = rikishi_id
            else:
                # keep None so the UPSERT won't overwrite existing non-null with null
                yusho_dict.setdefault(canon, None)
                if rikishi_id:
                    print(f"ℹ️ Yusho rikishi {rikishi_id} not found in rikishi table; leaving {canon} NULL.")

        insert_basho(cursor, basho_id, name, location, start_date, end_date, yusho_dict)
        conn.commit()

        for prize in data.get('specialPrizes', []):
            rid = prize.get('rikishiId')
            if rid and rikishi_exists(cursor, rid):
                insert_special_prize(cursor, basho_id, rid, prize.get('type'))
        conn.commit()
        cursor.close(); conn.close()

    files = [os.path.join(root, file)
             for root, dirs, files in os.walk(directory)
             for file in files if file.endswith('.json')]
    with ThreadPoolExecutor() as executor:
        list(executor.map(process_file, files))



# --- Insert function for rikishi_shikona_changes ---
def insert_shikona_change(cursor, rikishi_id, shikona, change_date, basho_id):
    cursor.execute(
        '''
        INSERT INTO rikishi_shikona_changes (rikishi_id, shikona, change_date, basho_id)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        ''',
        (rikishi_id, shikona, change_date, basho_id)
    )

# --- Process a rikishi shikona JSON file ---
def process_rikishi_shikona_json(directory):
    def rikishi_exists(cursor, rikishi_id):
        cursor.execute('SELECT 1 FROM rikishi WHERE id = %s', (rikishi_id,))
        return cursor.fetchone() is not None

    def basho_exists(cursor, basho_id):
        cursor.execute('SELECT 1 FROM basho WHERE id = %s', (basho_id,))
        return cursor.fetchone() is not None

    def process_file(json_path):
        conn = connect_to_database()
        cursor = conn.cursor()
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for record in data['records']:
            rikishi_id = record['rikishiId']
            basho_id = record['bashoId']
            if not rikishi_exists(cursor, rikishi_id):
                continue
            if not basho_exists(cursor, basho_id):
                continue
            shikona_en = record.get('shikonaEn', '')
            shikona_jp = record.get('shikonaJp', '')
            if shikona_jp and shikona_jp.strip():
                shikona = f"{shikona_en} ({shikona_jp})"
            else:
                shikona = shikona_en
            id_str = record['id']
            year = id_str[:4]
            month = id_str[4:6]
            change_date = f"{year}-{month}-01"
            insert_shikona_change(cursor, rikishi_id, shikona, change_date, basho_id)
        conn.commit()
        cursor.close()
        conn.close()
    files = [os.path.join(root, file)
             for root, dirs, files in os.walk(directory)
             for file in files if file.endswith('.json')]
    with ThreadPoolExecutor() as executor:
        list(executor.map(process_file, files))
        
# --- Insert function for rikishi_rank_history ---
def insert_rank_history(cursor, rikishi_id, rank_name, rank_value, rank_date, basho_id):
    cursor.execute(
        '''
        INSERT INTO rikishi_rank_history (rikishi_id, rank_name, rank_value, rank_date, basho_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        ''',
        (rikishi_id, rank_name, rank_value, rank_date, basho_id)
    )

# --- Process a rikishi ranks JSON file ---
def process_rikishi_ranks_json(directory):
    def rikishi_exists(cursor, rikishi_id):
        cursor.execute('SELECT 1 FROM rikishi WHERE id = %s', (rikishi_id,))
        return cursor.fetchone() is not None

    def basho_exists(cursor, basho_id):
        cursor.execute('SELECT 1 FROM basho WHERE id = %s', (basho_id,))
        return cursor.fetchone() is not None

    def process_file(json_path):
        conn = connect_to_database()
        cursor = conn.cursor()
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for record in data['records']:
            rikishi_id = record['rikishiId']
            basho_id = record['bashoId']
            if not rikishi_exists(cursor, rikishi_id):
                continue
            if not basho_exists(cursor, basho_id):
                continue
            rank_name = record['rank']
            rank_value = record['rankValue']
            id_str = record['id']
            year = id_str[:4]
            month = id_str[4:6]
            rank_date = f"{year}-{month}-01"
            insert_rank_history(cursor, rikishi_id, rank_name, rank_value, rank_date, basho_id)
        conn.commit()
        cursor.close()
        conn.close()
    files = [os.path.join(root, file)
             for root, dirs, files in os.walk(directory)
             for file in files if file.endswith('.json')]
    with ThreadPoolExecutor() as executor:
        list(executor.map(process_file, files))

# --- Insert function for rikishi_measurements_history ---
def insert_measurements_history(cursor, rikishi_id, height_cm, weight_kg, measurement_date, basho_id):
    cursor.execute(
        '''
        INSERT INTO rikishi_measurements_history (rikishi_id, height_cm, weight_kg, measurement_date, basho_id)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        ''',
        (rikishi_id, height_cm, weight_kg, measurement_date, basho_id)
    )

# --- Process a rikishi measurements JSON file ---
def process_rikishi_measurements_json(directory):
    def rikishi_exists(cursor, rikishi_id):
        cursor.execute('SELECT 1 FROM rikishi WHERE id = %s', (rikishi_id,))
        return cursor.fetchone() is not None

    def process_file(json_path):
        import psycopg2
        conn = connect_to_database()
        cursor = conn.cursor()
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        for record in data['records']:
            rikishi_id = record['rikishiId']
            if not rikishi_exists(cursor, rikishi_id):
                continue
            height_cm = record.get('height')
            weight_kg = record.get('weight')
            basho_id = record['bashoId']
            id_str = record['id']
            year = id_str[:4]
            month = id_str[4:6]
            measurement_date = f"{year}-{month}-01"
            try:
                insert_measurements_history(cursor, rikishi_id, height_cm, weight_kg, measurement_date, basho_id)
            except psycopg2.errors.ForeignKeyViolation as e:
                # Check if the error is due to missing basho_id
                if 'basho_id' in str(e) and 'is not present in table "basho"' in str(e):
                    conn.rollback()
                    # Insert minimal basho row
                    start_date = f"{basho_id[:4]}-{basho_id[4:6]}-01"
                    cursor.execute(
                        '''
                        INSERT INTO basho (id, start_date) VALUES (%s, %s)
                        ON CONFLICT (id) DO NOTHING;
                        ''',
                        (basho_id, start_date)
                    )
                    conn.commit()
                    # Retry the original insert
                    insert_measurements_history(cursor, rikishi_id, height_cm, weight_kg, measurement_date, basho_id)
                else:
                    raise
        conn.commit()
        cursor.close()
        conn.close()
    files = [os.path.join(root, file)
             for root, dirs, files in os.walk(directory)
             for file in files if file.endswith('.json')]
    with ThreadPoolExecutor() as executor:
        list(executor.map(process_file, files))

# --- Insert function for matches table ---
def insert_match(cursor, basho_id, division, day, match_number, east_id, east_shikona, east_rank, west_id, west_shikona, west_rank, winner_id, kimarite):
    cursor.execute(
        '''
        INSERT INTO matches (
            id, basho_id, division, day, match_number, east_rikishi_id, eastShikona, east_rank, west_rikishi_id, westShikona, west_rank, winner, kimarite
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
        ''',
        (int(str(basho_id) + str(day) + str(match_number) + str(east_id) + str(west_id)), basho_id, division, day, match_number, east_id, east_shikona, east_rank, west_id, west_shikona, west_rank, winner_id, kimarite)
    )

# --- Process rikishi_matches JSON files ---
def process_rikishi_matches_json(directory):
    def rikishi_exists(cursor, rikishi_id):
        if rikishi_id is None:
            return True
        cursor.execute('SELECT 1 FROM rikishi WHERE id = %s', (rikishi_id,))
        return cursor.fetchone() is not None

    def process_file(json_path):
        conn = connect_to_database()
        cursor = conn.cursor()
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception as e:
                print(f"⚠️ Skipping file {json_path}: JSON decode error: {e}")
                cursor.close()
                conn.close()
                return
        if not isinstance(data, dict) or 'records' not in data or not isinstance(data['records'], list):
            print(f"⚠️ Skipping file {json_path}: Not a dict or missing 'records' key.")
            cursor.close()
            conn.close()
            return
        for record in data.get('records', []):
            basho_id = int(record.get('bashoId')) if record.get('bashoId') else None
            division = record.get('division')
            day = record.get('day')
            match_number = record.get('matchNo')
            east_id = record.get('eastId')
            east_shikona = record.get('eastShikona')
            east_rank = record.get('eastRank')
            west_id = record.get('westId')
            west_shikona = record.get('westShikona')
            west_rank = record.get('westRank')
            winner_id = record.get('winnerId')
            kimarite = record.get('kimarite')
            # Only insert if all rikishi IDs exist (allow winner_id to be None)
            if not (rikishi_exists(cursor, east_id) and rikishi_exists(cursor, west_id) and (winner_id is None or rikishi_exists(cursor, winner_id))):
                continue
            insert_match(cursor, basho_id, division, day, match_number, east_id, east_shikona, east_rank, west_id, west_shikona, west_rank, winner_id, kimarite)
        conn.commit()
        cursor.close()
        conn.close()
    files = [os.path.join(root, file)
             for root, dirs, files in os.walk(directory)
             for file in files if file.endswith('.json')]
    with ThreadPoolExecutor() as executor:
        list(executor.map(process_file, files))


def process_rikishi_json_and_stats(rikishis_json_path, rikishi_stats_dir, cursor):
    import glob
    # Loop through all rikishi JSON files in the directory
    rikishi_files = [os.path.join(rikishis_json_path, f) for f in os.listdir(rikishis_json_path) if f.endswith('.json')]
    for rikishi_file in rikishi_files:
        with open(rikishi_file, 'r', encoding='utf-8') as f:
            record = json.load(f)
        rikishi_id = record['id']
        shikona_en = record.get('shikonaEn', '')
        shikona_jp = record.get('shikonaJp', '')
        shikona = f"{shikona_en} ({shikona_jp})" if shikona_jp else shikona_en
        birthdate = record.get('birthDate', None)
        if birthdate:
            birthdate = birthdate[:10]
        current_rank = record.get('currentRank')
        heya = record.get('heya')
        shusshin = record.get('shusshin')
        current_height = record.get('height')
        current_weight = record.get('weight')
        debut = record.get('debut')
        retirement_date = record.get('intai', None)
        if debut and len(debut) == 6:
            debut = f"{debut[:4]}-{debut[4:6]}-01"
        else:
            debut = None
        last_match = record.get('updatedAt', None)
        if last_match:
            last_match = last_match[:10]

        # Find and load stats file using glob pattern
        pattern = os.path.join(rikishi_stats_dir, f"rikishi_{rikishi_id}.json")
        matches_files = glob.glob(pattern)
        if matches_files:
            stats_path = matches_files[0]
            with open(stats_path, 'r', encoding='utf-8') as sf:
                stats = json.load(sf)
            basho_count = stats.get('basho')
            absent_count = stats.get('totalAbsences')
            wins = stats.get('totalWins')
            losses = stats.get('totalLosses')
            matches = stats.get('totalMatches')
            yusho_count = stats.get('yusho')
            sansho_count = sum(stats.get('sansho', {}).values()) if 'sansho' in stats else 0
        else:
            basho_count = absent_count = wins = losses = matches = yusho_count = sansho_count = None

        insert_rikishi(cursor, rikishi_id, shikona, birthdate, retirement_date, current_rank, heya, shusshin, current_height, current_weight, debut, last_match, basho_count, absent_count, wins, losses, matches, yusho_count, sansho_count)

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

# Call all process functions for each relevant directory
def populate_sumo_database(cursor):
    base_dir = 'c:/Users/Ryan Jewik/Desktop/sumo_app/data_ingestion/bucket_download/downloaded_s3/sumo-api-calls/'
    # Populate rikishi table first (sequential, as other tables depend on it)
    rikishi_dir = os.path.join(base_dir, 'rikishis')
    rikishi_stats_dir = os.path.join(base_dir, 'rikishi_stats')
    process_rikishi_json_and_stats(rikishi_dir, rikishi_stats_dir, cursor)
    conn.commit()
    print("✅ Rikishi table populated.")
    # Parallelize the rest
    process_basho_json(os.path.join(base_dir, 'basho'))
    conn.commit()
    print("✅ Basho table populated.")
    process_rikishi_shikona_json(os.path.join(base_dir, 'rikishi_shikonas'))
    conn.commit()
    print("✅ Rikishi shikona table populated.")
    process_rikishi_ranks_json(os.path.join(base_dir, 'rikishi_ranks'))
    conn.commit()
    print("✅ Rikishi ranks table populated.")
    process_rikishi_measurements_json(os.path.join(base_dir, 'rikishi_measurements'))
    conn.commit()
    print("✅ Rikishi measurements table populated.")
    process_rikishi_matches_json(os.path.join(base_dir, 'rikishi_matches'))
    conn.commit()
    print("✅ Rikishi matches table populated.")


populate_sumo_database(cursor)
conn.commit()