import psycopg2
load_dotenv()
import os, sys

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
            rank VARCHAR(50),
            country VARCHAR(100)
        );
        '''
        cursor.execute(create_rikishi_table_query)
        conn.commit()
        print("✅ Rikishi table ensured in database.")

        # Create basho table (mock)
        create_basho_table_query = '''
        CREATE TABLE IF NOT EXISTS basho (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100) NOT NULL,
            location VARCHAR(100),
            start_date DATE,
            end_date DATE
        );
        '''
        cursor.execute(create_basho_table_query)
        conn.commit()
        print("✅ Basho table ensured in database.")

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
        
        #populate tables--------------------------------------------------
        
        
    except Exception as e:
        print("❌ Error creating tables:", e)
    finally:
        cursor.close()
        conn.close()
