import psycopg2
from dotenv import load_dotenv
load_dotenv()
import os, sys, json
from pathlib import Path

#user database connection with retry mechanism
def connect_to_database(max_retries=30, delay=2):
    """
    Attempt to connect to the database with retry logic
    """
    for attempt in range(max_retries):
        try:
            print(f"Attempting to connect to database (attempt {attempt + 1}/{max_retries}):")
            print(f"  Host: {os.getenv('DB_HOST')}")
            print(f"  Database: {os.getenv('DB_NAME')}")
            print(f"  User: {os.getenv('DB_USERNAME')}")
            print(f"  Port: {os.getenv('DB_PORT')}")
            
            conn = psycopg2.connect(
                database=os.getenv("DB_NAME"),
                user=os.getenv("DB_USERNAME"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT")
            )
            print("✅ Database connection successful!")
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
    

if conn: #create tables------------------------------------------------------
    try:
        cursor = conn.cursor()
        
        # Enable uuid-ossp extension for UUID generation (if not already enabled)
        cursor.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";")
        conn.commit()


        # Create rikishi table first (id from sumo-api, not SERIAL)
        create_rikishi_table_query = '''
        CREATE TABLE IF NOT EXISTS rikishi (
            id INTEGER PRIMARY KEY,
            shikona VARCHAR(100) NOT NULL,
            birthdate DATE,
            current_rank VARCHAR(50),
            current_rank_value INT,
            heya VARCHAR(100),
            shusshin VARCHAR(100),
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
            name VARCHAR(100) NOT NULL,
            location VARCHAR(100),
            start_date DATE,
            end_date DATE
        );
        '''
        cursor.execute(create_basho_table_query)
        conn.commit()
        print("✅ Basho table ensured in database.")
        
        
        # Now create users table (UUID for id)
        create_users_table_query = '''
        CREATE TABLE IF NOT EXISTS users (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            username VARCHAR(50) UNIQUE NOT NULL,
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
            id SERIAL PRIMARY KEY,
            basho_id INT REFERENCES basho(id),
            east_rikishi_id INT REFERENCES rikishi(id),
            west_rikishi_id INT REFERENCES rikishi(id),
            winner VARCHAR(10),
            kimarite VARCHAR(50),
            match_date DATE
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
            match_id INT REFERENCES matches(id),
            predicted_winner VARCHAR(10),
            prediction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_correct BOOLEAN
        );
        '''
        cursor.execute(create_match_predictions_table_query)
        conn.commit()
        print("✅ Match predictions table ensured in database.")

        # Create rikishi_rank_history table
        create_rank_history_table_query = '''
        CREATE TABLE IF NOT EXISTS rikishi_rank_history (
            id SERIAL PRIMARY KEY,
            rikishi_id INT REFERENCES rikishi(id),
            rank_name VARCHAR(50) NOT NULL,
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
            shikona VARCHAR(100) NOT NULL,
            change_date DATE,
            basho_id INT REFERENCES basho(id)
        );
        '''
        cursor.execute(create_shikona_changes_table_query)
        conn.commit()
        print("✅ Rikishi shikona changes table ensured in database.")
    except Exception as e:
        print("❌ Error creating tables:", e)



#populate tables--------------------------------------------------
json_path = "../data_ingestion/bucket_download/downloaded_s3/sumo-api-calls/rikishi_shikonas/20250828T234414Z_rikishi_5_shikonas.json"

with open(json_path, "r", encoding="utf-8") as f:
    data = json.load(f)
for x in data['records']:
    print(x)
#shikonas populate
def shikonas_populate(data):
    for item in data['records']:
        # Extract relevant fields for shikona
        rikishi_id = item.get('rikishi_id')
        shikona = item.get('shikona')
        change_date = item.get('change_date')
        basho_id = item.get('basho_id')

        # Insert into the database
        insert_shikona_change(rikishi_id, shikona, change_date, basho_id)

#helper function to navigate json files
def process_json_files(directory):
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

def populate_sumo_database():
    process_json_files('c:/Users/Ryan Jewik/Desktop/sumo_app/data_ingestion/bucket_download/downloaded_s3/sumo-api-calls/rikishi/shinkonas')

# Example usage:
#populate_sumo_database()


cursor.close()
conn.close()