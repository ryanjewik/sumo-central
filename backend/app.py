
from flask import request, jsonify
import uuid

import psycopg2
from flask import Flask
import datetime
import os
from dotenv import load_dotenv
import time
from flask_cors import CORS
import re
import bcrypt


app = Flask(__name__)
CORS(app)
load_dotenv()



# User registration endpoint
@app.route('/api/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data.get('username', '').strip()
    email = data.get('email', '').strip()
    password = data.get('password', '')

    # Validate required fields
    if not username or not email or not password:
        return jsonify({'success': False, 'error': 'Please fill in all fields.'}), 400

    # Validate email format
    email_regex = r"^[\w\.-]+@[\w\.-]+\.\w+$"
    if not re.match(email_regex, email):
        return jsonify({'success': False, 'error': 'Invalid email format.'}), 400

    # Validate password: at least 10 chars, 1 number, 1 uppercase
    if len(password) < 10:
        return jsonify({'success': False, 'error': 'Password must be at least 10 characters.'}), 400
    if not re.search(r"[A-Z]", password):
        return jsonify({'success': False, 'error': 'Password must contain at least one uppercase letter.'}), 400
    if not re.search(r"\d", password):
        return jsonify({'success': False, 'error': 'Password must contain at least one number.'}), 400

    # Check if username or email already exists
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM users WHERE username = %s", (username,))
            if cur.fetchone():
                return jsonify({'success': False, 'error': 'Username is already taken.'}), 400
            cur.execute("SELECT 1 FROM users WHERE email = %s", (email,))
            if cur.fetchone():
                return jsonify({'success': False, 'error': 'Email is already registered.'}), 400

            # Hash password
            hashed_pw = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt())
            user_id = str(uuid.uuid4())
            created_at = datetime.datetime.utcnow()
            cur.execute(
                """
                INSERT INTO users (id, username, email, password, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (user_id, username, email, hashed_pw.decode('utf-8'), created_at)
            )
            conn.commit()
        return jsonify({'success': True}), 201
    except Exception as e:
        print('Error inserting user:', e)
        return jsonify({'success': False, 'error': 'Registration failed'}), 500

# User login endpoint
@app.route('/api/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data.get('username', '').strip()
    password = data.get('password', '')
    if not username or not password:
        return jsonify({'success': False, 'error': 'Please enter both username and password.'}), 400
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, username, password FROM users WHERE username = %s", (username,))
            row = cur.fetchone()
            if not row:
                return jsonify({'success': False, 'error': 'Invalid username or password.'}), 401
            user_id, username_db, hashed_pw = row[0], row[1], row[2].encode('utf-8')
            if not bcrypt.checkpw(password.encode('utf-8'), hashed_pw):
                return jsonify({'success': False, 'error': 'Invalid username or password.'}), 401
        return jsonify({'success': True, 'id': user_id, 'username': username_db}), 200
    except Exception as e:
        print('Login error:', e)
        return jsonify({'success': False, 'error': 'Login failed.'}), 500


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
            
            

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = os.getenv("MONGO_URI")

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)
            
            
if __name__ == "__main__":
    try:
        app.run(host='0.0.0.0', port=5000, debug=True)
    finally:
        # Close connections when the app shuts down
        if 'conn' in globals() and conn:
            conn.close()
            
            
            