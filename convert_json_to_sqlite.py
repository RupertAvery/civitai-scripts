import glob
import json
import sqlite3
from pathlib import Path

def create_tables(conn):
    cursor = conn.cursor()
    cursor.executescript("""
    CREATE TABLE IF NOT EXISTS creators (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE,
        image TEXT
    );

    CREATE TABLE IF NOT EXISTS models (
        id INTEGER PRIMARY KEY,
        name TEXT,
        description TEXT,
        allowNoCredit BOOLEAN,
        allowDerivatives BOOLEAN,
        allowDifferentLicense BOOLEAN,
        type TEXT,
        minor BOOLEAN,
        sfwOnly BOOLEAN,
        poi BOOLEAN,
        nsfw BOOLEAN,
        nsfwLevel INTEGER,
        availability TEXT,
        cosmetic TEXT,
        supportsGeneration BOOLEAN,
        creator_id INTEGER,
        downloadCount INTEGER,
        FOREIGN KEY (creator_id) REFERENCES creators(id)
    );

    CREATE TABLE IF NOT EXISTS modelVersions (
        id INTEGER PRIMARY KEY,
        model_id INTEGER,
        index_in_model INTEGER,
        name TEXT,
        baseModel TEXT,
        baseModelType TEXT,
        publishedAt TEXT,
        availability TEXT,
        nsfwLevel INTEGER,
        description TEXT,
        supportsGeneration BOOLEAN,
        downloadUrl TEXT,
        downloadCount INTEGER,
        FOREIGN KEY (model_id) REFERENCES models(id)
    );

    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY,
        sizeKB REAL,
        name TEXT,
        type TEXT,
        pickleScanResult TEXT,
        pickleScanMessage TEXT,
        virusScanResult TEXT,
        virusScanMessage TEXT,
        scannedAt TEXT,
        format TEXT,
        modelVersion_id INTEGER,
        sha256 TEXT,
        autov1 TEXT,
        autov2 TEXT,
        autov3 TEXT,
        crc32 TEXT,
        blake3 TEXT,
        downloadUrl TEXT,
        primaryFile BOOLEAN,
        FOREIGN KEY (modelVersion_id) REFERENCES modelversions(id)
    );

    CREATE TABLE IF NOT EXISTS tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        model_id INTEGER,
        tag TEXT,
        UNIQUE(model_id, tag),
        FOREIGN KEY (model_id) REFERENCES models(id)
    );
                            
    CREATE TABLE IF NOT EXISTS images (
        id INTEGER PRIMARY KEY,
        url TEXT,
        nsfwLevel INTEGER,
        width INTEGER,
        height INTEGER,
        hash TEXT,
        modelVersion_id INTEGER,
        FOREIGN KEY (modelVersion_id) REFERENCES modelversions(id)
    );                         
                            
    -- For fast lookup/filtering by name and type
    CREATE INDEX IF NOT EXISTS idx_models_name ON models(name);
    CREATE INDEX IF NOT EXISTS idx_models_type ON models(type);
    CREATE INDEX IF NOT EXISTS idx_models_nsfw ON models(nsfw);
    CREATE INDEX IF NOT EXISTS idx_models_sfwOnly ON models(sfwOnly);

    -- Foreign key from models → creators
    CREATE INDEX IF NOT EXISTS idx_models_creator_id ON models(creator_id);

    -- For fast filtering or joining on model version fields
    CREATE INDEX IF NOT EXISTS idx_modelversions_model_id ON modelversions(model_id);
    CREATE INDEX IF NOT EXISTS idx_modelversions_nsfwLevel ON modelversions(nsfwLevel);

    -- For fast access to files by model version
    CREATE INDEX IF NOT EXISTS idx_files_modelversion_id ON files(modelVersion_id);

    -- For tags lookup by model ID
    CREATE INDEX IF NOT EXISTS idx_tags_model_id ON tags(model_id);

    -- Optional: Indexes on creator fields if filtering/searching
    CREATE INDEX IF NOT EXISTS idx_creators_username ON creators(username);
                         
    CREATE INDEX IF NOT EXISTS idx_images_modelversion_id ON images(modelVersion_id);

    """)
    conn.commit()

def get_or_create_creator(conn, username, image):
    cursor = conn.cursor()
    cursor.execute("INSERT OR IGNORE INTO creators (username, image) VALUES (?, ?)", (username, image))
    conn.commit()
    cursor.execute("SELECT id FROM creators WHERE username = ?", (username,))
    return cursor.fetchone()[0]

def insert_model(conn, item, creator_id):
    stats = item.get("stats")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO models (id, name, description, allowNoCredit, allowDerivatives, 
                   allowDifferentLicense, type, minor, sfwOnly, poi, 
                   nsfw, nsfwLevel, availability, cosmetic, supportsGeneration, 
                   creator_id, downloadCount) 
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        item["id"],
        item.get("name"),
        item.get("description"),
        item.get("allowNoCredit"),
        item.get("allowDerivatives"),

        item.get("allowDifferentLicense"),
        item.get("type"),
        item.get("minor"),
        item.get("sfwOnly"),  
        item.get("poi"),
        
        item.get("nsfw"),
        item.get("nsfwLevel"),
        item.get("availability"),
        item.get("cosmetic"),
        item.get("supportsGeneration"),

        creator_id,
        stats.get("downloadCount"),
    ))
    conn.commit()

def insert_tags(conn, model_id, tags):
    cursor = conn.cursor()
    for tag in tags:
        cursor.execute("INSERT OR IGNORE INTO tags (model_id, tag) VALUES (?, ?)", (model_id, tag))
    conn.commit()

def insert_model_version(conn, version, model_id):
    stats = version.get("stats")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO modelVersions (
            id, model_id, index_in_model, name, baseModel, baseModelType,
            publishedAt, availability, nsfwLevel, description, supportsGeneration, 
            downloadUrl, downloadCount
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        version["id"], 
        model_id, 
        version["index"], 
        version["name"],
        version["baseModel"], 
        version.get("baseModelType"),

        version["publishedAt"], 
        version["availability"], 
        version["nsfwLevel"],
        version.get("description"), 
        version["supportsGeneration"], 

        version.get("downloadUrl"),
        stats.get("downloadCount"),
    ))
    conn.commit()
    return version["id"]

def insert_files(conn, files, modelVersion_id):
    cursor = conn.cursor()
    for file in files:
        hashes = file.get("hashes", {})
        metadata = file.get("metadata", {})
        cursor.execute("""
            INSERT OR IGNORE INTO files (
                id, sizeKB, name, type, pickleScanResult, pickleScanMessage,
                virusScanResult, virusScanMessage, scannedAt,
                format, modelVersion_id,
                sha256, autov1, autov2, autov3, crc32, blake3,
                downloadUrl, primaryFile
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            file["id"],
            file.get("sizeKB"),
            file.get("name"),
            file.get("type"),
            file.get("pickleScanResult"),
            file.get("pickleScanMessage"),
            file.get("virusScanResult"),
            file.get("virusScanMessage"),
            file.get("scannedAt"),
            metadata.get("format"),
            modelVersion_id,
            hashes.get("SHA256"),
            hashes.get("AutoV1"),
            hashes.get("AutoV2"),
            hashes.get("AutoV3"),
            hashes.get("CRC32"),
            hashes.get("BLAKE3"),
            file.get("downloadUrl"),
            file.get("primary")
        ))
    conn.commit()

def insert_images(conn, images, modelVersion_id):
    cursor = conn.cursor()
    for img in images:
        cursor.execute("""
            INSERT OR IGNORE INTO images (
                id, url, nsfwLevel, width, height, hash, modelVersion_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            img["id"],
            img.get("url"),
            img.get("nsfwLevel"),
            img.get("width"),
            img.get("height"),
            img.get("hash"),
            modelVersion_id
        ))
    conn.commit()    

def process_json(conn, json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)


    for item in data["items"]:
        creator_info = item.get("creator")
        creator_id = None
        if creator_info is not None and creator_info["username"] is not None:
            creator_id = get_or_create_creator(conn, creator_info["username"], creator_info["image"])
        insert_model(conn, item, creator_id)
        insert_tags(conn, item["id"], item["tags"])

        for version in item["modelVersions"]:
            version_id = insert_model_version(conn, version, item["id"])

            if "images" in version:
                insert_images(conn, version["images"], version["id"])

            insert_files(conn, version["files"], version_id)


if __name__ == "__main__":
    import sys
    db_path = "models.db"
    
    conn = sqlite3.connect(db_path)
    create_tables(conn)

    if len(sys.argv) == 2:
        process_json(conn, sys.argv[1])    

    else:
        for filepath in glob.glob("*.json"):
            print(f"Processing {filepath}...")
            try:
                process_json(conn, filepath)
            except json.JSONDecodeError as e:
                print(f"❌ Failed to parse {filepath}: {e}")
            except Exception as e:
                print(f"❌ Error processing {filepath}: {e}")

    conn.close()

    print(f"Data successfully inserted into '{db_path}'.")
