import time
from functools import wraps
import os
import logging  # Add logging import
import uuid
import jwt
import datetime
import requests
import json
import psycopg2
from psycopg2 import errors  # Import errors module
from psycopg2.extras import RealDictCursor, Json
from flask import (
    Flask,
    request,
    jsonify,
    send_from_directory,
    render_template,
)  # Added send_from_directory
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename  # Added for file uploads
from flask_cors import CORS
from flask_socketio import (
    SocketIO,
    emit,
    join_room,
    leave_room,
)  # Import SocketIO and related functions
import eventlet  # Import eventlet for async mode

eventlet.monkey_patch()  # Patch standard libraries for eventlet compatibility


# --- Timing Decorator ---
def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        total_time = end_time - start_time
        print(f"Function {func.__name__} Took {total_time:.4f} seconds")
        return result

    return timeit_wrapper


# --- Flask App Setup ---
app = Flask(__name__)
CORS(app)  # We'll configure CORS within SocketIO for simplicity

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - PID:%(process)d - %(message)s",
)

# --- Configuration ---
app.config["SECRET_KEY"] = os.environ.get(
    "SECRET_KEY", "your-secret-key"
)  # Change in production
if app.config["SECRET_KEY"] == "your-secret-key":
    print(
        "WARNING: Using default SECRET_KEY. Set the SECRET_KEY environment variable in production."
    )

# --- SocketIO Setup ---
# Configure CORS for SocketIO, allowing all origins for development.
# Restrict origins in production for security.
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="eventlet")

UPLOAD_FOLDER = "uploads/profile_pics"  # Define upload folder
ALLOWED_EXTENSIONS = {"png", "jpg", "jpeg", "gif"}
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
os.makedirs(
    app.config["UPLOAD_FOLDER"], exist_ok=True
)  # Create upload folder if it doesn't exist

# --- Database Configuration ---
DB_HOST = os.environ.get(
    "DB_HOST", "ep-proud-thunder-a8jk3ljg-pooler.eastus2.azure.neon.tech"
)
DB_NAME = os.environ.get("DB_NAME", "movie_watchlist")
DB_USER = os.environ.get("DB_USER", "neondb_owner")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "npg_X5DHWAhu1FOg")
DB_PORT = os.environ.get("DB_PORT", "5432")
DATABASE_URL = os.environ.get(
    "DATABASE_URL", f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
)
print(f"Connecting to PostgreSQL at {DATABASE_URL}")


# --- Database Connection ---
def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True  # Enable autocommit
    return conn


# --- Database Initialization ---
def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    # Create users table (Ensures basic table exists)
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS users (
        id SERIAL PRIMARY KEY,
        public_id VARCHAR(50) UNIQUE NOT NULL,
        username VARCHAR(50) UNIQUE NOT NULL,
        password VARCHAR(500) NOT NULL,
        -- Columns below might be added by ALTER TABLE if table already exists
        viewed_movies JSONB DEFAULT '[]',
        lists JSONB DEFAULT '[]'
    );
    """
    )  # Note: Removed bio/profile_pic from CREATE just to be safe, ALTER will add them

    # --- Add missing columns to users table robustly ---
    try:
        cur.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS bio TEXT DEFAULT '';")
        print("Checked/Added 'bio' column to users table.")
    except Exception as e:
        print(
            f"Error adding 'bio' column (might already exist with different type?): {e}"
        )
        # If ALTER fails, it might be okay if the column exists, but log it.
        # No rollback needed here as autocommit is likely on or implied by get_db_connection

    try:
        cur.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS profile_picture_url VARCHAR(500) DEFAULT NULL;"
        )
        print("Checked/Added 'profile_picture_url' column to users table.")
    except Exception as e:
        print(f"Error adding 'profile_picture_url' column: {e}")

    # Add back potentially missing JSONB columns if the initial CREATE was even older
    try:
        cur.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS viewed_movies JSONB DEFAULT '[]';"
        )
        print("Checked/Added 'viewed_movies' column to users table.")
    except Exception as e:
        print(f"Error adding 'viewed_movies' column: {e}")

    try:
        cur.execute(
            "ALTER TABLE users ADD COLUMN IF NOT EXISTS lists JSONB DEFAULT '[]';"
        )
        print("Checked/Added 'lists' column to users table.")
    except Exception as e:
        print(f"Error adding 'lists' column: {e}")

    # Create other tables... (rest of the function remains similar)
    # Create movies table
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS movies (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(50) NOT NULL,
        title VARCHAR(255) NOT NULL,
        director VARCHAR(255),
        year INTEGER,
        image_url VARCHAR(500)
    );
    """
    )

    # Create lists table (No changes needed for profile)
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS lists (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(50) NOT NULL,
        name VARCHAR(100) NOT NULL,
        movies JSONB DEFAULT '[]'
    );
    """
    )

    # Create reviews table (No changes needed for profile)
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS reviews (
        id SERIAL PRIMARY KEY,
        user_id VARCHAR(50) NOT NULL,
        movie_id INTEGER NOT NULL,
        rating INTEGER NOT NULL,
        comment TEXT
    );
    """
    )

    # Create followers table (New)
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS followers (
        follower_id VARCHAR(50) NOT NULL REFERENCES users(public_id) ON DELETE CASCADE,
        following_id VARCHAR(50) NOT NULL REFERENCES users(public_id) ON DELETE CASCADE,
        PRIMARY KEY (follower_id, following_id)
    );
    """
    )

    # --- Create Chat Tables ---
    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS chat_rooms (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255), -- For group chats
        is_group_chat BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    )
    print("Checked/Created 'chat_rooms' table.")

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS chat_participants (
        id SERIAL PRIMARY KEY,
        room_id INTEGER NOT NULL REFERENCES chat_rooms(id) ON DELETE CASCADE,
        user_id VARCHAR(50) NOT NULL REFERENCES users(public_id) ON DELETE CASCADE, -- Match users.public_id type
        joined_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
        last_read_timestamp TIMESTAMP WITH TIME ZONE,
        UNIQUE (room_id, user_id) -- Ensure a user is only once in a room
    );
    """
    )
    print("Checked/Created 'chat_participants' table.")

    cur.execute(
        """
    CREATE TABLE IF NOT EXISTS chat_messages (
        id BIGSERIAL PRIMARY KEY,
        room_id INTEGER NOT NULL REFERENCES chat_rooms(id) ON DELETE CASCADE,
        sender_id VARCHAR(50) NOT NULL REFERENCES users(public_id) ON DELETE CASCADE, -- Match users.public_id type
        content TEXT NOT NULL,
        message_type VARCHAR(20) DEFAULT 'text', -- e.g., 'text', 'image', 'system'
        sent_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );
    """
    )
    print("Checked/Created 'chat_messages' table.")

    # Add index for faster message retrieval by room
    cur.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_chat_messages_room_id_sent_at
    ON chat_messages (room_id, sent_at DESC);
    """
    )
    print("Checked/Created index on 'chat_messages'.")

    # Add index for faster participant lookup by user
    cur.execute(
        """
    CREATE INDEX IF NOT EXISTS idx_chat_participants_user_id
    ON chat_participants (user_id);
    """
    )
    print("Checked/Created index on 'chat_participants'.")

    cur.close()
    conn.close()


# Call init_db to ensure tables exist and add initial data
try:
    init_db()
    print("Database tables initialized/updated successfully")

    # --- Add Common Chat Room and Sample Messages ---
    conn = get_db_connection()
    cur = conn.cursor()

    # Check if common chat room (ID 0) exists
    cur.execute("SELECT id FROM chat_rooms WHERE id = 0;")
    common_chat_room = cur.fetchone()

    if common_chat_room is None:
        # Create common chat room if it doesn't exist
        # Use a specific ID (0) for the common chat
        try:
            cur.execute(
                "INSERT INTO chat_rooms (id, name, is_group_chat) VALUES (0, 'Common Chat', FALSE);"
            )
            print("Created Common Chat room with ID 0.")
        except psycopg2.Error:
            # Handle case where ID 0 might be used by another auto-generated room (unlikely but safe)
            print("Common Chat room with ID 0 already exists (unique violation).")
            conn.rollback()  # Rollback the failed insert
        except Exception as e:
            print(f"Error creating Common Chat room: {e}")
            conn.rollback()  # Rollback on other errors

    # Find the admin_user's public_id
    admin_user_public_id = None
    cur.execute("SELECT public_id FROM users WHERE id = 1 AND username = 'admin_user';")
    admin_user_row = cur.fetchone()
    if admin_user_row:
        admin_user_public_id = admin_user_row[0]
        print(f"Found admin_user with public_id: {admin_user_public_id}")
    else:
        print(
            "Admin user with id=1 and username='admin_user' not found. Cannot add sample messages."
        )

    if admin_user_public_id and common_chat_room is not None:
        # Check if admin_user is already a participant in room 0
        cur.execute(
            "SELECT id FROM chat_participants WHERE room_id = 0 AND user_id = %s;",
            (admin_user_public_id,),
        )
        participant_exists = cur.fetchone()

        if participant_exists is None:
            # Add admin_user as a participant to room 0
            try:
                cur.execute(
                    "INSERT INTO chat_participants (room_id, user_id) VALUES (0, %s);",
                    (admin_user_public_id,),
                )
                print(f"Added {admin_user_public_id} as participant to room 0.")
            except Exception as e:
                print(f"Error adding admin user as participant to room 0: {e}")
                conn.rollback()  # Rollback on error

        # Add sample messages if the room exists and admin user is found
        # Check if messages already exist in room 0 to avoid duplicates on restart
        cur.execute("SELECT COUNT(*) FROM chat_messages WHERE room_id = 0;")
        message_count = cur.fetchone()[0]

        if message_count == 0:
            print("Adding sample messages to Common Chat room.")
            sample_messages = [
                "Welcome to the Common Chat!",
                "This is a shared space for all users.",
                "Feel free to say hello!",
            ]
            for message_content in sample_messages:
                try:
                    cur.execute(
                        "INSERT INTO chat_messages (room_id, sender_id, content) VALUES (%s, %s, %s);",
                        (0, admin_user_public_id, message_content),
                    )
                except Exception as e:
                    print(f"Error inserting sample message: {e}")
                    conn.rollback()  # Rollback on error
            print("Sample messages added.")
        else:
            print(
                "Common Chat room already contains messages. Skipping sample message insertion."
            )

    conn.commit()  # Commit all changes in this block
    cur.close()
    conn.close()

except Exception as e:
    print(f"Error during database initialization or initial data population: {e}")
    # Note: If init_db itself failed, this outer block might catch it.
    # If subsequent data population fails, the inner rollbacks/commits handle it.


# --- Helper Functions ---
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


# --- Authentication Decorator ---
def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get("Authorization")
        if not token:
            return jsonify({"message": "Token is missing"}), 401
        try:
            # Assuming token is "Bearer <token>"
            if token.startswith("Bearer "):
                token = token.split(" ")[1]
            data = jwt.decode(token, app.config["SECRET_KEY"], algorithms=["HS256"])
            conn = get_db_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(
                "SELECT * FROM users WHERE public_id = %s", (data["public_id"],)
            )
            current_user = cur.fetchone()
            cur.close()
            conn.close()
            if not current_user:
                return jsonify({"message": "User not found"}), 401
        except jwt.ExpiredSignatureError:
            return jsonify({"message": "Token has expired"}), 401
        except jwt.InvalidTokenError:
            return jsonify({"message": "Token is invalid"}), 401
        except Exception as e:
            print(f"Token validation error: {e}")
            return jsonify({"message": "Token is invalid"}), 401
        return f(current_user, *args, **kwargs)

    return decorated


# --- Routes ---


# --- User Authentication ---
@app.route("/register", methods=["POST"])
@timeit
def register():
    data = request.get_json()
    username = data.get("username")
    password = data.get("password")

    if not username or not password:
        return jsonify({"message": "Missing username or password"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM users WHERE username = %s", (username,))
    existing_user = cur.fetchone()

    if existing_user:
        cur.close()
        conn.close()
        return jsonify({"message": "Username already exists"}), 400

    public_id = str(uuid.uuid4())
    hashed_password = generate_password_hash(password)

    cur.execute(
        "INSERT INTO users (public_id, username, password, bio, viewed_movies, lists) VALUES (%s, %s, %s, %s, %s, %s)",
        (public_id, username, hashed_password, "", "[]", "[]"),  # Initialize bio
    )
    cur.close()
    conn.close()
    return jsonify({"message": "User created successfully"}), 201


@app.route("/login", methods=["POST"])
@timeit
def login():
    auth = request.get_json()
    if not auth or not auth.get("username") or not auth.get("password"):
        return jsonify({"message": "Could not verify login details"}), 401

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM users WHERE username = %s", (auth["username"],))
    user = cur.fetchone()
    cur.close()
    conn.close()

    if not user:
        return jsonify({"message": "User not found"}), 401

    if check_password_hash(user["password"], auth["password"]):
        token = jwt.encode(
            {
                "public_id": user["public_id"],
                "exp": datetime.datetime.utcnow()
                + datetime.timedelta(hours=24),  # Extend token validity
            },
            app.config["SECRET_KEY"],
            algorithm="HS256",
        )
        # Return token without Bearer prefix
        return jsonify({"token": token, "success": True}), 200

    return jsonify({"message": "Incorrect password"}), 401

@app.route("/")
def home():
    return render_template("index.html")

# --- Profile Routes ---
@app.route("/profile", methods=["GET"])
@token_required
@timeit
def get_my_profile(current_user):
    conn = get_db_connection()
    cur = conn.cursor()
    user_public_id = current_user["public_id"]

    # Get following count (how many users the current user follows)
    cur.execute(
        "SELECT COUNT(*) FROM followers WHERE follower_id = %s", (user_public_id,)
    )
    following_count = cur.fetchone()[0]

    # Get follower count (how many users follow the current user)
    cur.execute(
        "SELECT COUNT(*) FROM followers WHERE following_id = %s", (user_public_id,)
    )
    follower_count = cur.fetchone()[0]

    cur.close()
    conn.close()

    # Return relevant profile fields, including counts
    profile_data = {
        "public_id": user_public_id,
        "username": current_user["username"],
        "bio": current_user.get("bio", ""),
        "profile_picture_url": current_user.get("profile_picture_url"),
        "following_count": following_count,
        "follower_count": follower_count,
    }
    return jsonify(profile_data), 200


@app.route("/profile", methods=["PUT"])
@token_required
@timeit
def update_my_profile(current_user):
    data = request.get_json()
    bio = data.get("bio")

    if bio is None:  # Allow updating only bio for now
        return jsonify({"message": "No update data provided"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "UPDATE users SET bio = %s WHERE public_id = %s",
        (bio, current_user["public_id"]),
    )
    cur.close()
    conn.close()

    return jsonify({"message": "Profile updated successfully"}), 200


@app.route("/profile/<public_id>", methods=["GET"])
@token_required  # Require login to view profiles
@timeit
def get_user_profile(current_user, public_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT public_id, username, bio, profile_picture_url FROM users WHERE public_id = %s",
        (public_id,),
    )
    user = cur.fetchone()

    if not user:
        cur.close()
        conn.close()
        return jsonify({"message": "User not found"}), 404

    # Check follow status
    cur.execute(
        "SELECT 1 FROM followers WHERE follower_id = %s AND following_id = %s",
        (current_user["public_id"], public_id),
    )
    is_following = cur.fetchone() is not None

    # Get follower/following counts
    cur.execute(
        "SELECT COUNT(*) as count FROM followers WHERE following_id = %s", (public_id,)
    )
    follower_count = cur.fetchone()["count"]
    cur.execute(
        "SELECT COUNT(*) as count FROM followers WHERE follower_id = %s", (public_id,)
    )
    following_count = cur.fetchone()["count"]

    cur.close()
    conn.close()

    user_data = dict(user)
    user_data["is_following"] = is_following
    user_data["follower_count"] = follower_count
    user_data["following_count"] = following_count

    return jsonify(user_data), 200


@app.route("/profile/picture", methods=["POST"])
@token_required
@timeit
def upload_profile_picture(current_user):
    if "file" not in request.files:
        return jsonify({"message": "No file part"}), 400
    file = request.files["file"]
    if file.filename == "":
        return jsonify({"message": "No selected file"}), 400
    if file and allowed_file(file.filename):
        filename = secure_filename(
            f"{current_user['public_id']}_{uuid.uuid4().hex[:8]}_{file.filename}"
        )
        filepath = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        try:
            file.save(filepath)
            # Store the relative URL path
            file_url = f"/uploads/profile_pics/{filename}"

            # Update user's profile_picture_url in DB
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "UPDATE users SET profile_picture_url = %s WHERE public_id = %s",
                (file_url, current_user["public_id"]),
            )
            cur.close()
            conn.close()

            return (
                jsonify(
                    {
                        "message": "File uploaded successfully",
                        "profile_picture_url": file_url,
                    }
                ),
                200,
            )
        except Exception as e:
            print(f"Error saving file or updating DB: {e}")
            # Clean up partially saved file if error occurs?
            if os.path.exists(filepath):
                os.remove(filepath)
            return jsonify({"message": "Failed to upload file"}), 500
    else:
        return jsonify({"message": "File type not allowed"}), 400


# Serve uploaded files (Needed for Flutter to display images)
@app.route("/uploads/profile_pics/<filename>")
def uploaded_file(filename):
    # Basic security check: ensure filename doesn't try to access parent directories
    if ".." in filename or filename.startswith("/"):
        return jsonify({"message": "Invalid filename"}), 400
    try:
        # Construct directory path relative to the app's root path
        directory = os.path.join(app.root_path, app.config["UPLOAD_FOLDER"])
        # Normalize the path for the current OS
        normalized_directory = os.path.normpath(directory)
        full_path = os.path.join(normalized_directory, filename)

        print(f"Normalized directory: {normalized_directory}")
        print(f"Attempting to serve file from full path: {full_path}")

        # Explicitly check if the file exists before sending
        if os.path.exists(full_path) and os.path.isfile(full_path):
            print(f"File found. Serving using send_from_directory.")
            # Use the normalized directory path
            return send_from_directory(normalized_directory, filename)
        else:
            print(f"File not found at path: {full_path}")
            return jsonify({"message": "File not found"}), 404

    except Exception as e:  # Catch other potential errors
        # Log the error properly
        print(f"Error serving file {filename} from {normalized_directory}: {e}")
        # Check if the error is specifically a NotFound exception from Werkzeug
        from werkzeug.exceptions import NotFound

        if isinstance(e, NotFound):
            print("Caught Werkzeug NotFound exception.")
            return jsonify({"message": "File not found (Werkzeug)"}), 404
        # Otherwise, return a generic 500 error
        return jsonify({"message": "Error serving file"}), 500


# --- Follow Routes ---
@app.route("/follow/<public_id>", methods=["POST"])
@token_required
@timeit
def follow_user(current_user, public_id):
    if current_user["public_id"] == public_id:
        return jsonify({"message": "You cannot follow yourself"}), 400

    conn = get_db_connection()
    cur = conn.cursor()

    # Check if user exists
    cur.execute("SELECT 1 FROM users WHERE public_id = %s", (public_id,))
    if not cur.fetchone():
        cur.close()
        conn.close()
        return jsonify({"message": "User to follow not found"}), 404

    # Attempt to insert follower relationship
    try:
        cur.execute(
            "INSERT INTO followers (follower_id, following_id) VALUES (%s, %s)",
            (current_user["public_id"], public_id),
        )
        message = "User followed successfully"
        status_code = 201
    except psycopg2.Error:  # Use imported errors module
        # Already following, treat as success or inform? Let's treat as success.
        message = "Already following this user"
        status_code = 200
    except Exception as e:
        print(f"Error following user: {e}")
        message = "Failed to follow user"
        status_code = 500

    cur.close()
    conn.close()
    return jsonify({"message": message}), status_code


@app.route("/unfollow/<public_id>", methods=["POST"])
@token_required
@timeit
def unfollow_user(current_user, public_id):
    conn = get_db_connection()
    cur = conn.cursor()

    # Attempt to delete follower relationship
    cur.execute(
        "DELETE FROM followers WHERE follower_id = %s AND following_id = %s RETURNING follower_id",
        (current_user["public_id"], public_id),
    )

    if cur.fetchone():  # Check if a row was actually deleted
        message = "User unfollowed successfully"
        status_code = 200
    else:
        # Either user doesn't exist or wasn't being followed
        message = "User not found or not being followed"
        status_code = 404  # Or 400 depending on desired behavior

    cur.close()
    conn.close()
    return jsonify({"message": message}), status_code


@app.route("/profile/<public_id>/followers", methods=["GET"])
@token_required
@timeit
def get_followers(current_user, public_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Get users who follow public_id
    cur.execute(
        """
        SELECT u.public_id, u.username, u.profile_picture_url
        FROM users u
        JOIN followers f ON u.public_id = f.follower_id
        WHERE f.following_id = %s
    """,
        (public_id,),
    )
    followers = cur.fetchall()

    cur.close()
    conn.close()
    return jsonify({"followers": followers}), 200


@app.route("/profile/<public_id>/following", methods=["GET"])
@token_required
@timeit
def get_following(current_user, public_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Get users whom public_id follows
    cur.execute(
        """
        SELECT u.public_id, u.username, u.profile_picture_url
        FROM users u
        JOIN followers f ON u.public_id = f.following_id
        WHERE f.follower_id = %s
    """,
        (public_id,),
    )
    following = cur.fetchall()

    cur.close()
    conn.close()
    return jsonify({"following": following}), 200


@app.route("/users", methods=["GET"])
@token_required
@timeit
def list_users(current_user):
    # Simple user listing (can add search/pagination later)
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # Exclude the current user from the list
    cur.execute(
        "SELECT public_id, username, profile_picture_url FROM users WHERE public_id != %s",
        (current_user["public_id"],),
    )
    users = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify({"users": users}), 200


@app.route("/api/users/search", methods=["GET"])
# @token_required
@timeit
def search_users():
    username_query = request.args.get("username")
    if not username_query:
        return jsonify({"message": "Missing username query parameter"}), 400
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # Use ILIKE for case-insensitive search and % for partial matching
    cur.execute(
        "SELECT public_id, username, bio, profile_picture_url FROM users WHERE username ILIKE %s LIMIT 20",
        (f"%{username_query}%",),
    )
    users = cur.fetchall()
    cur.close()
    conn.close()

    return jsonify({"users": users}), 200


# --- Movie/List/Review Routes (Mostly unchanged, check token handling) ---


@app.route("/movies", methods=["GET"])
@token_required
@timeit
def get_movies(current_user):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM movies WHERE user_id = %s", (current_user["public_id"],))
    movies = cur.fetchall()

    output = []
    for movie in movies:
        image_url = movie.get("image_url")  # Use .get for safety
        output.append(
            {
                "title": movie["title"],
                "director": movie.get("director"),
                "year": movie.get("year"),
                "_id": str(movie["id"]),  # Keep as string for consistency
                "imageUrl": image_url,
            }
        )

    # Update viewed_movies (Consider if this logic is still desired here)
    viewed_movies = current_user.get("viewed_movies", [])
    if not isinstance(viewed_movies, list):
        try:
            viewed_movies = json.loads(viewed_movies) if viewed_movies else []
        except json.JSONDecodeError:
            viewed_movies = []  # Handle potential malformed JSON

    updated = False
    for movie in movies:
        if movie["id"] not in viewed_movies:
            viewed_movies.append(movie["id"])
            updated = True

    if updated:
        cur.execute(
            "UPDATE users SET viewed_movies = %s WHERE public_id = %s",
            (Json(viewed_movies), current_user["public_id"]),
        )

    cur.close()
    conn.close()
    return jsonify({"movies": output}), 200


@app.route("/movies/<int:movie_id>", methods=["DELETE"])  # Use int converter
@token_required
@timeit
def delete_movie(current_user, movie_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT user_id FROM movies WHERE id = %s", (movie_id,))
    movie = cur.fetchone()

    if not movie:
        cur.close()
        conn.close()
        return jsonify({"message": "Movie not found"}), 404

    if movie["user_id"] != current_user["public_id"]:
        cur.close()
        conn.close()
        return jsonify({"message": "Unauthorized to delete this movie"}), 403

    cur.execute("DELETE FROM movies WHERE id = %s", (movie_id,))
    # Consider deleting associated reviews as well?
    # cur.execute("DELETE FROM reviews WHERE movie_id = %s AND user_id = %s", (movie_id, current_user["public_id"]))

    cur.close()
    conn.close()
    return jsonify({"message": "Movie deleted successfully"}), 200


@app.route("/movies", methods=["POST"])
@token_required
@timeit
def add_movie(current_user):
    data = request.get_json()
    if not data or "title" not in data or "director" not in data or "year" not in data:
        return (
            jsonify({"message": "Missing movie data (title, director, year required)"}),
            400,
        )

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO movies (user_id, title, director, year, image_url) VALUES (%s, %s, %s, %s, %s) RETURNING id",
        (
            current_user["public_id"],
            data["title"],
            data["director"],
            data["year"],
            data.get("poster_path"),  # poster_path is optional
        ),
    )
    new_movie_id = cur.fetchone()[0]
    cur.close()
    conn.close()
    return jsonify({"message": "Movie added successfully", "id": new_movie_id}), 201


@app.route("/movies/<int:movie_id>/reviews", methods=["POST"])  # Use int converter
@token_required
@timeit
def add_review(current_user, movie_id):
    data = request.get_json()
    if not data or "rating" not in data:
        return jsonify({"message": "Missing rating data"}), 400

    conn = get_db_connection()
    cur = conn.cursor()
    # Check if movie exists (optional but good practice)
    cur.execute("SELECT 1 FROM movies WHERE id = %s", (movie_id,))
    if not cur.fetchone():
        cur.close()
        conn.close()
        return jsonify({"message": "Movie not found"}), 404

    cur.execute(
        "INSERT INTO reviews (user_id, movie_id, rating, comment) VALUES (%s, %s, %s, %s)",
        (
            current_user["public_id"],
            movie_id,
            data["rating"],
            data.get("comment", ""),
        ),  # comment is optional
    )
    cur.close()
    conn.close()
    return jsonify({"message": "Review added successfully"}), 201


@app.route("/movies/<int:movie_id>/reviews", methods=["GET"])  # Use int converter
@token_required  # Should reviews be public or require login? Let's require login.
@timeit
def get_reviews(current_user, movie_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # Join with users table to get username
    cur.execute(
        """
        SELECT r.id, r.rating, r.comment, r.user_id, u.username, u.profile_picture_url
        FROM reviews r
        JOIN users u ON r.user_id = u.public_id
        WHERE r.movie_id = %s
        ORDER BY r.id DESC
        """,
        (movie_id,),
    )
    reviews = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify({"reviews": reviews}), 200


@app.route("/movies/<int:movie_id>")  # Use int converter
@token_required  # Require login to view details?
@timeit
def get_movie(current_user, movie_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("SELECT * FROM movies WHERE id = %s", (movie_id,))
    movie = cur.fetchone()
    cur.close()
    conn.close()

    if not movie:
        return jsonify({"message": "Movie not found"}), 404

    # --- TMDB Integration (Optional, keep if needed) ---
    tmdb_api_key = os.environ.get(
        "TMDB_API_KEY", "9e52747ca54fd6c620a126a000f36c7c"
    )  # Use your actual key
    movie_details = None
    watch_providers = None
    if tmdb_api_key and movie.get("title"):
        try:
            # Search TMDB
            search_url = f"https://api.themoviedb.org/3/search/movie?api_key={tmdb_api_key}&query={movie['title']}&year={movie.get('year','')}"
            search_response = requests.get(search_url, timeout=5)
            search_response.raise_for_status()
            search_data = search_response.json()

            if search_data.get("results"):
                tmdb_movie_id = search_data["results"][0]["id"]
                # Get Details
                movie_url = f"https://api.themoviedb.org/3/movie/{tmdb_movie_id}?api_key={tmdb_api_key}"
                movie_response = requests.get(movie_url, timeout=5)
                movie_response.raise_for_status()
                movie_details = movie_response.json()
                # Get Providers
                providers_url = f"https://api.themoviedb.org/3/movie/{tmdb_movie_id}/watch/providers?api_key={tmdb_api_key}"
                providers_response = requests.get(providers_url, timeout=5)
                providers_response.raise_for_status()
                watch_providers = providers_response.json().get("results")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching TMDB data for '{movie.get('title')}': {e}")
        except Exception as e:
            print(f"Unexpected error during TMDB fetch: {e}")
    # --- End TMDB Integration ---

    movie_data = {
        "id": movie["id"],
        "user_id": movie["user_id"],
        "title": movie["title"],
        "director": movie.get("director"),
        "year": movie.get("year"),
        "image_url": movie.get("image_url"),
        "tmdb_data": movie_details,
        "watch_providers": watch_providers,
    }
    return jsonify({"movie": movie_data}), 200


@app.route("/recommendations")
@token_required
@timeit
def get_recommendations(current_user):
    # Simple recommendation: movies not yet in user's list
    # (Could be improved with more complex logic later)
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Get IDs of movies the user *has* added to their lists/watched
    cur.execute(
        "SELECT id FROM movies WHERE user_id = %s", (current_user["public_id"],)
    )
    user_movie_ids = {row["id"] for row in cur.fetchall()}

    # Get all movies *not* added by the current user
    cur.execute(
        "SELECT id, title, director, year, image_url FROM movies WHERE user_id != %s",
        (current_user["public_id"],),
    )
    all_other_movies = cur.fetchall()

    # Filter out movies the user might have added from other users (if sharing is implemented)
    # For now, just recommend movies from other users they haven't added themselves.
    # A better approach might involve collaborative filtering or content-based recommendations.
    recommended_movies = [
        movie for movie in all_other_movies if movie["id"] not in user_movie_ids
    ]

    # Limit recommendations for performance/display
    recommendations_limit = 10
    output = []
    for movie in recommended_movies[:recommendations_limit]:
        output.append(
            {
                "title": movie["title"],
                "director": movie.get("director"),
                "year": movie.get("year"),
                "_id": str(movie["id"]),  # Keep as string
                "imageUrl": movie.get("image_url"),
            }
        )

    cur.close()
    conn.close()
    return jsonify({"recommendations": output}), 200


@app.route("/user/movie_count", methods=["GET"])
@token_required
@timeit
def get_movie_count(current_user):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "SELECT COUNT(*) as movie_count FROM movies WHERE user_id = %s",
        (current_user["public_id"],),
    )
    result = cur.fetchone()
    cur.close()
    conn.close()
    return jsonify({"movie_count": result["movie_count"] if result else 0}), 200


@app.route("/lists", methods=["POST"])
@token_required
@timeit
def create_list(current_user):
    data = request.get_json()
    list_name = data.get("name")
    if not list_name:
        return jsonify({"message": "List name is required"}), 400

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        "INSERT INTO lists (user_id, name, movies) VALUES (%s, %s, %s) RETURNING id",
        (current_user["public_id"], list_name, "[]"),
    )
    new_list_id = cur.fetchone()["id"]

    # Update user's lists column (Optional, maybe remove if not used)
    # user_lists = current_user.get("lists", [])
    # if not isinstance(user_lists, list):
    #     try: user_lists = json.loads(user_lists) if user_lists else []
    #     except: user_lists = []
    # user_lists.append(new_list_id)
    # cur.execute("UPDATE users SET lists = %s WHERE public_id = %s", (Json(user_lists), current_user["public_id"]))

    cur.close()
    conn.close()
    return jsonify({"message": "List created successfully", "id": new_list_id}), 201


@app.route("/lists", methods=["GET"])
@token_required
@timeit
def get_lists(current_user):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # Assuming 'lists' column in 'users' table stores list metadata (id, name)
    # Or query the separate 'lists' table if that's the structure
    # Let's stick to querying the dedicated 'lists' table as per the original code structure
    cur.execute(
        "SELECT id, name FROM lists WHERE user_id = %s", (current_user["public_id"],)
    )
    user_lists = cur.fetchall()
    cur.close()
    conn.close()
    # Return in the format expected by the original code
    return jsonify({"lists": user_lists}), 200


# --- YENİ ENDPOINT: Belirli bir listenin detaylarını ve filmlerini getirir ---
@app.route("/lists/<int:list_id>", methods=["GET"])
@token_required
@timeit
def get_list_details(current_user, list_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 1. Listenin varlığını ve kullanıcıya ait olduğunu kontrol et
    cur.execute(
        "SELECT id, name, movies FROM lists WHERE id = %s AND user_id = %s",
        (list_id, current_user["public_id"]),
    )
    list_data = cur.fetchone()

    if not list_data:
        cur.close()
        conn.close()
        return jsonify({"message": "List not found or you are not authorized"}), 404

    movie_ids = list_data.get("movies", [])
    movies_in_list = []

    # 2. Listede film ID'leri varsa, bu filmlerin detaylarını getir
    if movie_ids:
        # movie_ids listesi boşsa SQL hatası almamak için kontrol
        cur.execute(
            "SELECT id, title, director, year, image_url FROM movies WHERE id = ANY(%s)",
            (movie_ids,),
        )
        movies_in_list = cur.fetchall()

    cur.close()
    conn.close()

    # Filmleri Flutter'ın beklediği formata dönüştür
    output_movies = []
    for movie in movies_in_list:
        output_movies.append(
            {
                "_id": str(movie["id"]),
                "title": movie["title"],
                "director": movie.get("director"),
                "year": movie.get("year"),
                "imageUrl": movie.get("image_url"),
            }
        )

    return (
        jsonify(
            {"id": list_data["id"], "name": list_data["name"], "movies": output_movies}
        ),
        200,
    )


# --- YENİ ENDPOINT: Bir filme ait veriyi alıp, movies tablosuna ekler ve listeye referansını kaydeder ---
@app.route("/lists/<int:list_id>/add-movie", methods=["POST"])
@token_required
@timeit
def add_movie_to_list(current_user, list_id):
    data = request.get_json()
    # Gerekli film verilerini al
    if not data or "title" not in data or "director" not in data or "year" not in data:
        return (
            jsonify({"message": "Missing movie data (title, director, year required)"}),
            400,
        )

    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # 1. Listenin varlığını ve kullanıcıya ait olduğunu kontrol et
        cur.execute(
            "SELECT movies FROM lists WHERE id = %s AND user_id = %s",
            (list_id, current_user["public_id"]),
        )
        list_data = cur.fetchone()
        if not list_data:
            return jsonify({"message": "List not found or you are not authorized"}), 404

        # 2. Filmi ana `movies` tablosuna ekle
        # Not: Aynı filmin tekrar tekrar eklenmesini önlemek için bir kontrol eklenebilir.
        # Şimdilik basit tutuyoruz.
        cur.execute(
            "INSERT INTO movies (user_id, title, director, year, image_url) VALUES (%s, %s, %s, %s, %s) RETURNING id",
            (
                current_user["public_id"],
                data["title"],
                data["director"],
                data["year"],
                data.get("poster_path"),
            ),
        )
        new_movie_id = cur.fetchone()["id"]

        # 3. Listenin mevcut film dizisini al
        current_movie_ids = list_data.get("movies", [])
        if new_movie_id not in current_movie_ids:
            current_movie_ids.append(new_movie_id)

        # 4. Listenin film dizisini yeni film ID'si ile güncelle
        cur.execute(
            "UPDATE lists SET movies = %s WHERE id = %s",
            (Json(current_movie_ids), list_id),
        )

        # autocommit=True olduğu için conn.commit() gerekmez

        return (
            jsonify(
                {
                    "message": "Movie added to list successfully",
                    "movie_id": new_movie_id,
                }
            ),
            201,
        )

    except Exception as e:
        # conn.rollback() autocommit modunda anlamsızdır ama hata ayıklama için yararlı
        print(f"Error adding movie to list: {e}")
        return jsonify({"message": "An internal error occurred"}), 500
    finally:
        cur.close()
        conn.close()


# --- Chat REST API Endpoints ---


@app.route("/api/chats", methods=["GET"])
@token_required
@timeit
def get_user_chats(current_user):
    """Fetches the list of chat rooms the current user is part of."""
    user_public_id = current_user["public_id"]
    chats = []
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # Query to get rooms the user participates in,
        # and determine the display name (either group name or other user's name)
        cur.execute(
            """
            SELECT
                cr.id,
                cr.is_group_chat,
                cr.created_at,
                cr.updated_at,
                cr.name AS original_group_name, -- The original name from chat_rooms table
                CASE
                    WHEN cr.is_group_chat = TRUE THEN cr.name -- Use the group's name
                    ELSE other_user.username                 -- Use the other user's username for 1-on-1
                END AS display_name
            FROM
                chat_rooms cr
            JOIN
                chat_participants cp_current_user ON cr.id = cp_current_user.room_id AND cp_current_user.user_id = %s
            LEFT JOIN -- To find the other participant in 1-on-1 chats
                chat_participants other_participant ON cr.id = other_participant.room_id AND other_participant.user_id != %s AND cr.is_group_chat = FALSE
            LEFT JOIN -- To get the other participant's username
                users other_user ON other_participant.user_id = other_user.public_id AND cr.is_group_chat = FALSE
            ORDER BY
                cr.updated_at DESC;
            """,
            (user_public_id, user_public_id),  # user_public_id is used twice
        )
        chats_raw = cur.fetchall()
        logging.info(f"Raw chats fetched for user {user_public_id}: {chats_raw}")

        chats = []
        for chat_data in chats_raw:
            # Convert RealDictRow to a mutable dict for modification
            chat = dict(chat_data)
            if chat.get("created_at"):
                chat["created_at"] = chat["created_at"].isoformat()
            if chat.get("updated_at"):
                chat["updated_at"] = chat["updated_at"].isoformat()

            # Fallback for group chats if name is somehow null
            if chat.get("is_group_chat") and not chat.get("display_name"):
                chat["display_name"] = "Group Chat"

            # Ensure display_name is not None for 1-on-1 if other_user.username was NULL (should not happen if users exist)
            if not chat.get("is_group_chat") and chat.get("display_name") is None:
                chat["display_name"] = (
                    "Chat"  # Fallback for 1-on-1 if other user's name is missing
                )

            chats.append(chat)

        logging.info(f"Processed chats for user {user_public_id}: {chats}")

        cur.close()
        conn.close()
        return jsonify({"chats": chats}), 200

    except psycopg2.Error as db_err:
        print(f"Database error fetching chats for user {user_public_id}: {db_err}")
        return (
            jsonify({"message": "Failed to retrieve chat list due to database error."}),
            500,
        )
    except Exception as e:
        print(f"Unexpected error fetching chats for user {user_public_id}: {e}")
        return jsonify({"message": "An unexpected server error occurred."}), 500


@app.route("/api/chats/<int:room_id>/messages", methods=["GET"])
@token_required
@timeit
def get_chat_messages(current_user, room_id):
    """Fetches messages for a specific chat room with pagination."""
    user_public_id = current_user["public_id"]
    limit = request.args.get("limit", default=50, type=int)
    # Use 'before_message_id' for cursor-based pagination (more robust than timestamp)
    before_message_id = request.args.get("before_message_id", default=None, type=int)

    # Ensure limit is reasonable
    if limit <= 0 or limit > 200:
        limit = 50

    messages = []
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Verify user is participant in the room
        # cur.execute(
        #     "SELECT 1 FROM chat_participants WHERE room_id = %s AND user_id = %s",
        #     (room_id, user_public_id),
        # )
        # if not cur.fetchone():
        #     cur.close()
        #     conn.close()
        #     return (
        #         jsonify({"message": "Not authorized to view messages in this room."}),
        #         403,
        #     )  # Forbidden

        # 2. Build the query for messages
        query = """
            SELECT
                cm.id,
                cm.room_id,
                cm.sender_id,
                u.username AS sender_username, -- Get sender's username
                cm.content,
                cm.message_type,
                cm.sent_at
            FROM chat_messages cm
            JOIN users u ON cm.sender_id = u.public_id -- Join to get username
            WHERE cm.room_id = %s
        """
        params = [room_id]

        # Add condition for pagination if 'before_message_id' is provided
        if before_message_id is not None:
            query += " AND cm.id < %s"
            params.append(before_message_id)

        query += " ORDER BY cm.sent_at DESC, cm.id DESC LIMIT %s;"  # Order by time, then ID as tie-breaker
        params.append(limit)

        cur.execute(query, tuple(params))
        messages = cur.fetchall()

        # Convert datetime objects to ISO format strings
        for msg in messages:
            if msg.get("sent_at"):
                msg["sent_at"] = msg["sent_at"].isoformat()
            # Structure sender info
            msg["sender"] = {
                "id": msg.pop("sender_id"),
                "username": msg.pop("sender_username"),
            }

        cur.close()
        conn.close()
        return jsonify({"messages": messages}), 200

    except psycopg2.Error as db_err:
        print(f"Database error fetching messages for room {room_id}: {db_err}")
        return (
            jsonify({"message": "Failed to retrieve messages due to database error."}),
            500,
        )
    except Exception as e:
        print(f"Unexpected error fetching messages for room {room_id}: {e}")
        return jsonify({"message": "An unexpected server error occurred."}), 500


@app.route("/api/chats", methods=["POST"])
@token_required
@timeit
def create_chat_room(current_user):
    """Creates a new chat room (one-to-one or group)."""
    user_public_id = current_user["public_id"]
    data = request.get_json()

    participant_ids = data.get("participant_ids")  # List of public_ids
    is_group_chat = data.get("is_group_chat", False)
    group_name = data.get("name", None)  # Optional name for group chats

    # --- Input Validation ---
    if not participant_ids or not isinstance(participant_ids, list):
        return jsonify({"message": "Missing or invalid 'participant_ids' list."}), 400

    # Ensure the creator is always included and remove duplicates
    all_participant_ids = list(set([user_public_id] + participant_ids))

    if len(all_participant_ids) < 2:
        return (
            jsonify({"message": "At least two participants are required for a chat."}),
            400,
        )

    if not is_group_chat and len(all_participant_ids) != 2:
        return (
            jsonify(
                {"message": "One-to-one chats must have exactly two participants."}
            ),
            400,
        )

    if is_group_chat and not group_name:
        # Optionally generate a default name or require one
        # For now, let's allow unnamed groups
        pass

    conn = None  # Initialize conn to None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # --- Verify Participants Exist ---
        # Use a placeholder string for the IN clause
        placeholders = ", ".join(["%s"] * len(all_participant_ids))
        cur.execute(
            f"SELECT public_id FROM users WHERE public_id IN ({placeholders})",
            tuple(all_participant_ids),
        )
        found_users = cur.fetchall()
        if len(found_users) != len(all_participant_ids):
            found_ids = {u["public_id"] for u in found_users}
            missing_ids = [pid for pid in all_participant_ids if pid not in found_ids]
            cur.close()
            conn.close()
            return (
                jsonify({"message": f"Some participant IDs not found: {missing_ids}"}),
                404,
            )

        # --- Handle One-to-One Chat Creation ---
        if not is_group_chat:
            user_a_id = all_participant_ids[0]
            user_b_id = all_participant_ids[1]
            # Ensure consistent order for checking existing rooms
            if user_a_id > user_b_id:
                user_a_id, user_b_id = user_b_id, user_a_id

            # Check if a 1-on-1 room already exists
            cur.execute(
                """
                SELECT cp1.room_id
                FROM chat_participants cp1
                JOIN chat_participants cp2 ON cp1.room_id = cp2.room_id
                JOIN chat_rooms cr ON cp1.room_id = cr.id
                WHERE cp1.user_id = %s
                  AND cp2.user_id = %s
                  AND cr.is_group_chat = FALSE;
                """,
                (user_a_id, user_b_id),
            )
            existing_room = cur.fetchone()

            if existing_room:
                cur.close()
                conn.close()
                return (
                    jsonify(
                        {
                            "message": "Chat room already exists.",
                            "room_id": existing_room["room_id"],
                        }
                    ),
                    200,
                )  # OK, return existing

            # Create new 1-on-1 room
            cur.execute(
                "INSERT INTO chat_rooms (is_group_chat) VALUES (FALSE) RETURNING id;"
            )
            new_room_id = cur.fetchone()["id"]

            # Add participants
            cur.execute(
                "INSERT INTO chat_participants (room_id, user_id) VALUES (%s, %s), (%s, %s);",
                (new_room_id, user_a_id, new_room_id, user_b_id),
            )
            # Autocommit handles this

            cur.close()
            conn.close()
            # Also emit to the involved users via SocketIO if they are connected
            # This requires mapping user_public_id to active sids
            # (Implementation omitted for brevity, requires shared state like Redis or careful handling of connected_users dict)
            return (
                jsonify(
                    {
                        "message": "Chat room created successfully.",
                        "room_id": new_room_id,
                    }
                ),
                201,
            )  # Created

        # --- Handle Group Chat Creation ---
        else:
            # Create new group room
            cur.execute(
                "INSERT INTO chat_rooms (name, is_group_chat) VALUES (%s, TRUE) RETURNING id;",
                (group_name,),
            )
            new_room_id = cur.fetchone()["id"]

            # Prepare participant data for insertion
            participant_values = [(new_room_id, pid) for pid in all_participant_ids]
            placeholders = ", ".join(["(%s, %s)"] * len(participant_values))
            # Flatten the list of tuples for execute
            flat_values = [item for tpl in participant_values for item in tpl]

            # Add participants using executemany or constructing the query string
            cur.execute(
                f"INSERT INTO chat_participants (room_id, user_id) VALUES {placeholders};",
                tuple(flat_values),
            )
            # Autocommit handles this

            cur.close()
            conn.close()
            # Also emit to the involved users via SocketIO if they are connected
            return (
                jsonify(
                    {
                        "message": "Group chat room created successfully.",
                        "room_id": new_room_id,
                    }
                ),
                201,
            )  # Created

    except psycopg2.Error as db_err:
        print(f"Database error creating chat room: {db_err}")
        if conn:  # Close connection if it was opened
            cur.close()
            conn.close()
        return (
            jsonify({"message": "Failed to create chat room due to database error."}),
            500,
        )
    except Exception as e:
        print(f"Unexpected error creating chat room: {e}")
        if conn:  # Close connection if it was opened
            cur.close()
            conn.close()
        return jsonify({"message": "An unexpected server error occurred."}), 500


# --- Helper: User Info Storage (Simple In-Memory) ---
# WARNING: This is not suitable for production with multiple workers.
# Use Redis or another shared store in production.
connected_users = {}  # sid: {'public_id': ..., 'username': ...}
# TODO: Replace in-memory connected_users with a shared store like Redis for multi-worker setups.


# --- Helper: Get User from Token ---
def get_user_from_token(token):
    """Validates JWT token and returns user info or None."""
    if not token:
        return None
    try:
        # Ensure the token doesn't have "Bearer " prefix if passed directly
        if token.startswith("Bearer "):
            token = token.split(" ")[1]
        data = jwt.decode(token, app.config["SECRET_KEY"], algorithms=["HS256"])
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Fetch necessary user info
        cur.execute(
            "SELECT public_id, username FROM users WHERE public_id = %s",
            (data["public_id"],),
        )
        user = cur.fetchone()
        cur.close()
        conn.close()
        return user
    except jwt.ExpiredSignatureError:
        print("Token validation error: ExpiredSignatureError")
        return None
    except jwt.InvalidTokenError:
        print("Token validation error: InvalidTokenError")
        return None
    except Exception as e:
        print(f"Unexpected error during token validation: {e}")
        return None


# --- Sample Data (Optional) ---
@timeit
def add_sample_data():
    # ... (keep sample data logic if desired, ensure it uses new schema)
    print("Sample data logic skipped for brevity.")


# --- SocketIO Event Handlers ---


@socketio.on("connect")
def handle_connect():
    """Handles new client connections, authenticates, and joins rooms."""
    token = request.args.get("token")  # Get token from query param ?token=...
    user = get_user_from_token(token)

    if not user:
        logging.warning(
            f"Connection rejected: Invalid or missing token for SID {request.sid}"
        )
        return False  # Reject connection by returning False

    user_public_id = user["public_id"]
    connected_users[request.sid] = user  # Store user info mapped to sid
    logging.info(
        f"Client connected: SID={request.sid}, User={user['username']}({user_public_id}), PID={os.getpid()}"
    )

    # Join rooms the user is part of
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        # Select room IDs where the user is a participant
        cur.execute(
            "SELECT room_id FROM chat_participants WHERE user_id = %s",
            (user_public_id,),
        )
        rooms = cur.fetchall()
        cur.close()
        conn.close()

        for room in rooms:
            room_id_str = str(room["room_id"])  # SocketIO rooms are typically strings
            join_room(room_id_str)
            logging.info(
                f"User {user_public_id} (SID={request.sid}) joined room {room_id_str}"
            )

        # Optional: Notify others that the user is online
        # emit('user_status', {'user_id': user_public_id, 'is_online': True}, broadcast=True, include_self=False) # Needs refinement on who to notify

    except Exception as e:
        logging.error(
            f"Error joining rooms for user {user_public_id} (SID={request.sid}) on connect: {e}",
            exc_info=True,
        )
        # Consider disconnecting the user if joining rooms fails critically
        # disconnect(request.sid)
        # return False


@socketio.on("disconnect")
def handle_disconnect():
    """Handles client disconnections and cleans up."""
    user_info = connected_users.pop(request.sid, None)  # Remove user from mapping
    if user_info:
        user_public_id = user_info["public_id"]
        logging.info(
            f"Client disconnected: SID={request.sid}, User={user_info['username']}({user_public_id}), PID={os.getpid()}"
        )
        # Optional: Notify others that the user is offline
        # emit('user_status', {'user_id': user_public_id, 'is_online': False, 'last_seen': datetime.datetime.utcnow().isoformat()}, broadcast=True) # Needs refinement
    else:
        logging.warning(
            f"Client disconnected: SID={request.sid} (User info not found, might have failed connect), PID={os.getpid()}"
        )


@socketio.on("send_message")
def handle_send_message(data):
    """Handles incoming messages, saves them, and broadcasts."""
    user_info = connected_users.get(request.sid)
    if not user_info:
        logging.warning(
            f"Message rejected: User not authenticated for SID {request.sid}"
        )
        # Optionally emit an error back to the specific client
        emit("message_error", {"message": "Authentication required. Please reconnect."})
        return

    room_id = data.get("room_id")
    content = data.get("content")

    # Basic validation
    # Basic validation and type casting for room_id
    if not content or len(content.strip()) == 0:
        emit("message_error", {"message": "Message content cannot be empty."})
        return

    try:
        # Attempt to cast room_id to integer
        room_id_int = int(room_id)
    except (ValueError, TypeError):
        emit(
            "message_error",
            {"message": "Invalid input: room_id must be an integer."},
        )
        return

    # Now use room_id_int for database operations and further logic
    room_id = room_id_int  # Use the validated integer version

    sender_id = user_info["public_id"]
    sender_username = user_info["username"]

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. Verify user is participant in the room (Skip for common chat room 0)
        if room_id != 0:
            print(f"Checking participation for user {sender_id} in room {room_id}")
            cur.execute(
                "SELECT 1 FROM chat_participants WHERE room_id = %s AND user_id = %s",
                (room_id, sender_id),
            )
            if not cur.fetchone():
                cur.close()
                conn.close()
                emit(
                    "message_error",
                    {"message": "Not authorized to send messages in this room."},
                )
                return

        # 2. Insert the message
        cur.execute(
            """
            INSERT INTO chat_messages (room_id, sender_id, content)
            VALUES (%s, %s, %s)
            RETURNING id, room_id, sender_id, content, message_type, sent_at
            """,
            (room_id, sender_id, content.strip()),  # Use stripped content
        )
        new_message = cur.fetchone()
        # Autocommit is enabled, so no explicit commit needed here
        cur.close()
        conn.close()

        if new_message:
            # 3. Prepare payload for broadcasting
            message_payload = {
                "id": new_message["id"],
                "room_id": new_message["room_id"],
                "sender": {  # Include sender info
                    "id": sender_id,
                    "username": sender_username,
                },
                "content": new_message["content"],
                "message_type": new_message["message_type"],
                # Convert datetime to ISO 8601 string format for JSON serialization
                "sent_at": new_message["sent_at"].isoformat(),
            }
            room_id_str = str(room_id)  # Use string for room name
            # Emit to the specific room
            socketio.emit("new_message", message_payload, room=room_id_str)
            logging.info(
                f"Message {new_message['id']} sent by {sender_id} to room {room_id_str}"
            )
        else:
            logging.error(
                f"Error: Failed to save message from {sender_id} to room {room_id}."
            )
            emit("message_error", {"message": "Failed to save message to database."})

    except psycopg2.Error as db_err:
        logging.error(
            f"Database error handling send_message for room {room_id}: {db_err}",
            exc_info=True,
        )
        emit("message_error", {"message": "A database error occurred."})
    except Exception as e:
        logging.error(
            f"Unexpected error handling send_message for room {room_id}: {e}",
            exc_info=True,
        )
        emit("message_error", {"message": "An unexpected server error occurred."})


# --- Main Execution ---
if __name__ == "__main__":
    # Use socketio.run to start the server with WebSocket support
    print("Starting Flask-SocketIO server with eventlet...")
    # Use host='0.0.0.0' to make it accessible externally
    # debug=True enables auto-reloading but might cause issues with eventlet in some setups.
    # Consider setting use_reloader=False if you encounter problems.
    # Using port 5001 as intended for the chat service
    socketio.run(
        app, host="0.0.0.0", port=5003, debug=True, use_reloader=False
    )  # Added use_reloader=False for stability with eventlet
