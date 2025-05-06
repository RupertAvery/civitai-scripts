import glob
import json
import sqlite3
from pathlib import Path

def ensure_column_exists(conn, table, column, col_type="TEXT"):
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table})")
    columns = [row[1] for row in cursor.fetchall()]
    if column not in columns:
        print(f"⚠️ Adding missing column '{column}' to '{table}'...")
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type}")
        conn.commit()

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

    CREATE INDEX IF NOT EXISTS idx_models_name ON models(name);
    CREATE INDEX IF NOT EXISTS idx_models_type ON models(type);
    CREATE INDEX IF NOT EXISTS idx_models_nsfw ON models(nsfw);
    CREATE INDEX IF NOT EXISTS idx_models_sfwOnly ON models(sfwOnly);
    CREATE INDEX IF NOT EXISTS idx_models_downloadCount ON models(downloadCount);
    CREATE INDEX IF NOT EXISTS idx_models_creator_id ON models(creator_id);
    CREATE INDEX IF NOT EXISTS idx_modelversions_model_id ON modelversions(model_id);
    CREATE INDEX IF NOT EXISTS idx_modelversions_nsfwLevel ON modelversions(nsfwLevel);
    CREATE INDEX IF NOT EXISTS idx_modelversions_baseModel ON modelversions(baseModel);
    CREATE INDEX IF NOT EXISTS idx_modelversions_publishedAt ON modelversions(publishedAt);
    CREATE INDEX IF NOT EXISTS idx_modelversions_downloadCount ON modelversions(downloadCount);
    CREATE INDEX IF NOT EXISTS idx_files_modelversion_id ON files(modelVersion_id);
    CREATE INDEX IF NOT EXISTS idx_files_sha256 ON files(sha256);
    CREATE INDEX IF NOT EXISTS idx_files_autov1 ON files(autov1);
    CREATE INDEX IF NOT EXISTS idx_files_autov2 ON files(autov2);
    CREATE INDEX IF NOT EXISTS idx_files_autov3 ON files(autov3);
    CREATE INDEX IF NOT EXISTS idx_files_crc32 ON files(crc32);
    CREATE INDEX IF NOT EXISTS idx_files_blake3 ON files(blake3);
    CREATE INDEX IF NOT EXISTS idx_tags_model_id ON tags(model_id);
    CREATE INDEX IF NOT EXISTS idx_tags_tag ON tags(tag);
    CREATE INDEX IF NOT EXISTS idx_creators_username ON creators(username);
    CREATE INDEX IF NOT EXISTS idx_images_modelversion_id ON images(modelVersion_id);

    """)
    conn.commit()

def get_or_create_creator(conn, username, image):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO creators (username, image) 
        VALUES (?, ?) 
        ON CONFLICT(username) DO UPDATE SET image=excluded.image
    """, (username, image))
    # conn.commit()
    cursor.execute("SELECT id FROM creators WHERE username = ?", (username,))
    return cursor.fetchone()[0]

def insert_model(conn, item, creator_id):
    stats = item.get("stats")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO models (id, name, description, allowNoCredit, allowDerivatives, 
                   allowDifferentLicense, type, minor, sfwOnly, poi, 
                   nsfw, nsfwLevel, availability, cosmetic, supportsGeneration, 
                   creator_id, downloadCount) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            name=excluded.name,
            description=excluded.description,
            allowNoCredit=excluded.allowNoCredit,
            allowDerivatives=excluded.allowDerivatives,
            allowDifferentLicense=excluded.allowDifferentLicense,
            type=excluded.type,
            minor=excluded.minor,
            sfwOnly=excluded.sfwOnly,
            poi=excluded.poi,
            nsfw=excluded.nsfw,
            nsfwLevel=excluded.nsfwLevel,
            availability=excluded.availability,
            cosmetic=excluded.cosmetic,
            supportsGeneration=excluded.supportsGeneration,
            creator_id=excluded.creator_id,
            downloadCount=excluded.downloadCount
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
    # conn.commit()

def insert_tags(conn, model_id, tags):
    cursor = conn.cursor()
    for tag in tags:
        cursor.execute("""
            INSERT INTO tags (model_id, tag)
            VALUES (?, ?) 
            ON CONFLICT(model_id, tag) DO NOTHING
        """, (model_id, tag))
    # conn.commit()

def insert_model_version(conn, version, model_id):
    stats = version.get("stats")
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO modelVersions (
            id, model_id, index_in_model, name, baseModel, baseModelType,
            publishedAt, availability, nsfwLevel, description, supportsGeneration, 
            downloadUrl, downloadCount
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(id) DO UPDATE SET
            model_id=excluded.model_id,
            index_in_model=excluded.index_in_model,
            name=excluded.name,
            baseModel=excluded.baseModel,
            baseModelType=excluded.baseModelType,
            publishedAt=excluded.publishedAt,
            availability=excluded.availability,
            nsfwLevel=excluded.nsfwLevel,
            description=excluded.description,
            supportsGeneration=excluded.supportsGeneration,
            downloadUrl=excluded.downloadUrl,
            downloadCount=excluded.downloadCount
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
    # conn.commit()
    return version["id"]

def insert_files(conn, files, modelVersion_id):
    cursor = conn.cursor()
    for file in files:
        hashes = file.get("hashes", {})
        metadata = file.get("metadata", {})
        cursor.execute("""
            INSERT INTO files (
                id, sizeKB, name, type, pickleScanResult, pickleScanMessage,
                virusScanResult, virusScanMessage, scannedAt,
                format, modelVersion_id,
                sha256, autov1, autov2, autov3, crc32, blake3,
                downloadUrl, primaryFile
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                sizeKB=excluded.sizeKB,
                name=excluded.name,
                type=excluded.type,
                pickleScanResult=excluded.pickleScanResult,
                pickleScanMessage=excluded.pickleScanMessage,
                virusScanResult=excluded.virusScanResult,
                virusScanMessage=excluded.virusScanMessage,
                scannedAt=excluded.scannedAt,
                format=excluded.format,
                modelVersion_id=excluded.modelVersion_id,
                sha256=excluded.sha256,
                autov1=excluded.autov1,
                autov2=excluded.autov2,
                autov3=excluded.autov3,
                crc32=excluded.crc32,
                blake3=excluded.blake3,
                downloadUrl=excluded.downloadUrl,
                primaryFile=excluded.primaryFile
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
    # conn.commit()

def insert_images(conn, images, modelVersion_id):
    cursor = conn.cursor()
    for img in images:
        cursor.execute("""
            INSERT INTO images (
                id, url, nsfwLevel, width, height, hash, modelVersion_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                url=excluded.url,
                nsfwLevel=excluded.nsfwLevel,
                width=excluded.width,
                height=excluded.height,
                hash=excluded.hash,
                modelVersion_id=excluded.modelVersion_id
        """, (
            img["id"],
            img.get("url"),
            img.get("nsfwLevel"),
            img.get("width"),
            img.get("height"),
            img.get("hash"),
            modelVersion_id
        ))
    # conn.commit()

def process_json(conn, json_path):
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    
    conn.execute("BEGIN")
    
    try:
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
        conn.commit()
    except Exception:
        conn.rollback()
        raise

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