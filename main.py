import psycopg2
import json

# Database configuration
DB_CONFIG = {
    "dbname": "rickemorty",
    "user": "postgres",
    "password": "123456",
    "host": "localhost",
    "port": 5432
}

try:
    # Connecting to the database
    conn = psycopg2.connect(**DB_CONFIG)
    print("Database connection established.")
    
    # Cursor for executing SQL commands
    cur = conn.cursor()
    
    # Inserting data into the location table
    with open("allLocations.json", "r") as file:
        locations = json.load(file)
    locations = sorted(locations, key=lambda x: x["id"])  # Sorting by ID
    insert_location = """
    INSERT INTO location (id, name, type, dimension, residents_count) 
    VALUES (%s, %s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;
    """
    for loc in locations:
        cur.execute(insert_location, (
            loc["id"], 
            loc["name"], 
            loc.get("type", None), 
            loc.get("dimension", None), 
            loc.get("residents_count", 0)
        ))
    print("Location data inserted successfully.")

    # Inserting data into the episode table    
    with open("allEpisodesUpdated.json", "r") as file:
        episodes = json.load(file)
    episodes = sorted(episodes, key=lambda x: x["id"])  # Sorting by ID
    insert_episode = """
    INSERT INTO episode (id, name, air_date, episode) 
    VALUES (%s, %s, %s, %s) ON CONFLICT (id) DO NOTHING;
    """
    for ep in episodes:
        cur.execute(insert_episode, (
            ep["id"], 
            ep["name"], 
            ep.get("air_date", None), 
            ep.get("episode", None)
        ))
    print("Episode data inserted successfully.")
    
    # Inserting data into the character table
    with open("allCharsUpdated.json", "r") as file:
        characters = json.load(file)
    characters = sorted(characters, key=lambda x: x["id"])  # Sorting by ID
    insert_character = """
    INSERT INTO character (id, name, status, species, type, gender, image, origin_id, location_id) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
    ON CONFLICT (id) DO NOTHING;
    """
    for char in characters:
        origin_id = char.get("origin", {}).get("url", "").rstrip("/").split("/")[-1]
        origin_id = int(origin_id) if origin_id.isdigit() else None

        location_id = char.get("location", {}).get("url", "").rstrip("/").split("/")[-1]
        location_id = int(location_id) if location_id.isdigit() else None

        cur.execute(insert_character, (
            char["id"], 
            char["name"], 
            char.get("status", None), 
            char.get("species", None), 
            char.get("type", None), 
            char.get("gender", None), 
            char.get("image", None), 
            origin_id,
            location_id
        ))
    print("Character data inserted successfully.")
    
    #Inserting data into the character_episode table
    insert_character_episode = """
    INSERT INTO character_episode (episode_id, character_id) 
    VALUES (%s, %s) ON CONFLICT DO NOTHING;
    """
    for char in characters:
        for episode_url in char.get("episode", []):
            episode_id = episode_url.rstrip("/").split("/")[-1]
            if episode_id.isdigit():
                cur.execute(insert_character_episode, (int(episode_id), char["id"]))
    
    conn.commit()
    print("Character and episode relationship data inserted successfully.")

except Exception as e:
    print(f"Error: {e}")

finally:
    if 'cur' in locals():
        cur.close()
    if 'conn' in locals():
        conn.close()