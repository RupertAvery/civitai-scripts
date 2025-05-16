import os
import sqlite3
import pandas as pd
import requests
import re
import time
from pathlib import Path
from urllib.parse import urlparse
from tqdm import tqdm

ERROR_LOG_PATH = Path("download_errors.log")

def log_error(model_name, version_id, file_type, reason):
    message = f"[SKIP] model: {model_name} | version_id: {version_id} | type: {file_type} | reason: {reason}"
    with open(ERROR_LOG_PATH, "a", encoding="utf-8") as log_file:
        log_file.write(message.strip() + "\n")

MAX_FOLDER_NAME_LENGTH = 100  # safe limit under Windows path constraints

def sanitize_if_needed(name, max_len=MAX_FOLDER_NAME_LENGTH):
    if len(name) <= max_len:
        clean = name
    else:
        clean = ''.join(c for c in name if 32 <= ord(c) <= 126)

    # Replace or remove illegal Windows folder characters
    clean = clean.replace('\\', '_')  # Backslash
    clean = clean.replace('/', '_')   # Slash
    clean = clean.replace(':', '-')   # Colon
    clean = clean.replace('*', '')    # Asterisk
    clean = clean.replace('?', '')    # Question mark
    clean = clean.replace('"', '')    # Double quote
    clean = clean.replace('<', '')    # Less-than
    clean = clean.replace('>', '')    # Greater-than
    clean = clean.replace('|', '')    # Pipe
    clean = re.sub(r'\s+', ' ', clean)  # Collapse multiple spaces
    clean = clean.strip()  # Remove leading/trailing spaces

    return clean[:max_len]

# === BASE MODEL OPTIONS (Database Reference) ===
ALL_KNOWN_BASE_MODELS = {
    'SD 1.4', 'SD 1.5', 'Other', 'SD 2.1', 'SD 2.0 768', 'SD 2.1 768', 'SD 2.0',
    'Pony', 'SDXL 0.9', 'SDXL 1.0', 'SDXL Distilled', 'SDXL Turbo', 'SDXL 1.0 LCM',
    'Playground v2', 'PixArt a', 'SD 1.5 LCM', 'Stable Cascade', 'Illustrious',
    'SVD XT', 'SDXL Lightning', 'SD 2.1 Unclip', 'ODOR', 'Flux.1 D', 'Mochi',
    'SDXL Hyper', 'SD 1.5 Hyper', 'PixArt E', 'Flux.1 S', 'AuraFlow', 'SD 3',
    'Hunyuan 1', 'Kolors', 'Lumina', 'NoobAI', 'SD 3.5', 'SD 3.5 Medium',
    'SD 3.5 Large', 'CogVideoX', 'Hunyuan Video', 'SD 3.5 Large Turbo', 'HiDream',
    'Wan Video', 'LTXV'
}

# === CONFIG ===
DB_PATH = "models.db"
OUTPUT_DIR = "Downloaded/"

# Optional Civitai API key for private/restricted models
CIVITAI_API_KEY = "YOUR_CIVITAI_API_HERE"  # e.g. "sk-abc123..." or leave blank for anonymous access

# Allow or block NSFW images/models
ALLOW_NSFW = False #True or False

# Base model filter
SELECTED_BASE_MODEL = "all"  # e.g. "SD 1.4", or ["SD 1.4", "Pony"], or "all"
IGNORED_BASE_MODELS = []     # e.g. ["SD 1.4", "Other"]
#Done: list your finished base model lora's here for convenient memo

# Skip these model IDs manually
SKIP_MODEL_IDS = []

# Automatically skip models that are:
SKIP_DUPLICATE_NAMES = False
SKIP_TOO_LARGE_ID = 9999990000

def sanitize_filename(name):
    name = re.sub(r'[^\w\s\-\.]', '', name, flags=re.ASCII)
    return re.sub(r'\s+', ' ', name).strip()

# === RESOLVE FOCUS_BASE_MODELS ===
if SELECTED_BASE_MODEL != "all" and IGNORED_BASE_MODELS:
    raise ValueError("❌ You can't set both SELECTED_BASE_MODEL and IGNORED_BASE_MODELS at the same time.")

if isinstance(SELECTED_BASE_MODEL, str) and SELECTED_BASE_MODEL.lower() == "all":
    if IGNORED_BASE_MODELS:
        FOCUS_BASE_MODELS = ALL_KNOWN_BASE_MODELS - {
            bm for bm in ALL_KNOWN_BASE_MODELS
            if any(bm.lower() == ign.lower() for ign in IGNORED_BASE_MODELS)
        }
    else:
        FOCUS_BASE_MODELS = None
elif isinstance(SELECTED_BASE_MODEL, str):
    FOCUS_BASE_MODELS = {
        bm for bm in ALL_KNOWN_BASE_MODELS if bm.lower() == SELECTED_BASE_MODEL.lower()
    }
    if not FOCUS_BASE_MODELS:
        raise ValueError(f"❌ Unknown base model: '{SELECTED_BASE_MODEL}'")
elif isinstance(SELECTED_BASE_MODEL, list):
    FOCUS_BASE_MODELS = {
        bm for bm in ALL_KNOWN_BASE_MODELS
        if any(bm.lower() == sel.lower() for sel in SELECTED_BASE_MODEL)
    }
    if not FOCUS_BASE_MODELS:
        raise ValueError(f"❌ No matching base models found in: {SELECTED_BASE_MODEL}")
else:
    raise TypeError("❌ SELECTED_BASE_MODEL must be 'all', a string, or a list of strings.")

def download_file(url, output_folder, fallback_name=None, model_name="Unknown", version_id="?", file_type="unknown"):
    if not url:
        return

    headers = {}
    if CIVITAI_API_KEY:
        headers["Authorization"] = f"Bearer {CIVITAI_API_KEY}"

    max_retries = 3
    delay_seconds = 2

    for attempt in range(1, max_retries + 1):
        try:
            # Use API key on first try, fallback to anon if 401/403
            r = requests.get(url, timeout=15, allow_redirects=True, headers=headers if attempt == 1 else {})

            if r.status_code == 200:
                cd = r.headers.get("content-disposition", "")
                filename = fallback_name
                if "filename=" in cd:
                    filename = cd.split("filename=")[-1].strip().strip('"')
                elif not filename:
                    filename = os.path.basename(urlparse(url).path) or "file.bin"

                output_path = output_folder / filename
                if not output_path.exists():
                    with open(output_path, "wb") as f:
                        f.write(r.content)
                    print(f"📥 Downloaded: {filename}")
                return  # ✅ Success, exit function

            elif r.status_code in (401, 403) and attempt == 1 and CIVITAI_API_KEY:
                tqdm.write(f"🔓 API key failed for {model_name} | version {version_id} | type: {file_type} — retrying anonymously.")
                continue  # Retry with anonymous

            else:
                reason = f"HTTP {r.status_code}"
                print(f"⚠️ Attempt {attempt}/{max_retries} failed ({reason})")
                if attempt == max_retries:
                    log_error(model_name, version_id, file_type, reason)

        except Exception as e:
            reason = str(e)
            print(f"⚠️ Attempt {attempt}/{max_retries} failed with exception: {reason}")
            if attempt == max_retries:
                log_error(model_name, version_id, file_type, reason)

        time.sleep(delay_seconds)

# === Load Database ===
conn = sqlite3.connect(DB_PATH)
df_models = pd.read_sql_query("SELECT * FROM models", conn)
df_versions = pd.read_sql_query("SELECT * FROM modelVersions", conn)
df_images = pd.read_sql_query("SELECT * FROM images", conn)
df_tags = pd.read_sql_query("SELECT * FROM tags", conn)
df_trained_words = pd.read_sql_query("SELECT * FROM trainedWords", conn)
conn.close()

# === Verify Schema ===
required_model_columns = {"id", "name"}
required_version_columns = {"id", "model_id", "baseModel"}
if not required_model_columns.issubset(df_models.columns):
    raise Exception(f"models table is missing columns: {required_model_columns - set(df_models.columns)}")
if not required_version_columns.issubset(df_versions.columns):
    raise Exception(f"modelVersions table is missing columns: {required_version_columns - set(df_versions.columns)}")

# === Merge models with versions ===
merged = pd.merge(df_versions, df_models, left_on='model_id', right_on='id', suffixes=('_version', '_model'))
merged['baseModel'] = merged['baseModel'].fillna('Unknown')

print("🔍 Found base models in DB:", sorted(merged['baseModel'].dropna().unique()))

# === Process by base model ===
for base_model in merged['baseModel'].unique():
    if FOCUS_BASE_MODELS and base_model not in FOCUS_BASE_MODELS:
        print(f"⏭️ Skipping {base_model} (not in focus list)")
        continue

    base_folder = Path(OUTPUT_DIR) / base_model
    base_folder.mkdir(parents=True, exist_ok=True)
    base_versions = df_versions[df_versions['baseModel'].str.lower() == base_model.lower()]
    model_ids = base_versions['model_id'].unique()
    base_models = merged[merged['model_id'].isin(model_ids)]

    print(f"🔍 Filtering by base model: {base_model} | Matched model IDs: {len(model_ids)}")

    seen_names = set()

    model_ids = base_models['model_id'].unique()
    tqdm.write(f"📦 Exporting {len(model_ids)} model(s) from base model: {base_model}")

    for model_id in tqdm(model_ids, desc=f"{base_model}", unit="model"):
        if model_id in SKIP_MODEL_IDS:
            tqdm.write(f"⏭️ Skipped manually: model ID {model_id}")
            continue
        if model_id > SKIP_TOO_LARGE_ID:
            tqdm.write(f"⏭️ Skipped large ID: model ID {model_id}")
            continue

        model_row = df_models[df_models['id'] == model_id].iloc[0]
        model_name = sanitize_if_needed(model_row['name'])
        model_folder = base_folder / model_name

        if SKIP_DUPLICATE_NAMES:
            name_key = f"{model_name}_{base_model}"
            if name_key in seen_names:
                tqdm.write(f"⏭️ Skipped duplicate: {name_key}")
                continue
            seen_names.add(name_key)

        if model_folder.exists():
            tqdm.write(f"✅ Already exported: {model_name} ({base_model})")
            continue

        model_folder.mkdir(parents=True, exist_ok=True)

        versions = df_versions[df_versions['model_id'] == model_id]
        version_ids = versions['id'].tolist()

        images = df_images[df_images['modelVersion_id'].isin(version_ids)]
        tags = df_tags[df_tags['model_id'] == model_id]
        words = df_trained_words[df_trained_words['modelVersion_id'].isin(version_ids)]

        model_row.to_frame().T.to_json(model_folder / "model.json", orient="records", indent=2)
        versions.to_json(model_folder / "modelVersions.json", orient="records", indent=2)
        images.to_json(model_folder / "images.json", orient="records", indent=2)
        tags.to_json(model_folder / "tags.json", orient="records", indent=2)
        words.to_json(model_folder / "trainedWords.json", orient="records", indent=2)

        image_folder = model_folder / "images"
        image_folder.mkdir(exist_ok=True)

        for _, row in images.iterrows():
            nsfw_level = row.get("nsfwLevel", 0)
            if not ALLOW_NSFW and nsfw_level and int(nsfw_level) >= 2:
                print(f"🚫 Skipping NSFW image ID {row.get('id')} for model {model_name}")
                continue

            url = row.get("url")
            image_id = row.get("id")
            hash_val = row.get("hash", "")
            filename = os.path.basename(urlparse(url).path) if url else f"{hash_val}.jpg"

            download_file(url, image_folder, fallback_name=filename, model_name=model_name, version_id=row.get("modelVersion_id"), file_type="image")

            metadata = {
                "id": image_id,
                "url": url,
                "filename": filename,
                "hash": hash_val,
                "nsfwLevel": row.get("nsfwLevel"),
                "width": row.get("width"),
                "height": row.get("height"),
                "modelVersion_id": row.get("modelVersion_id"),
            }

            metadata_path = image_folder / f"image_{image_id}.json"
            with open(metadata_path, "w", encoding="utf-8") as f:
                f.write(pd.Series(metadata).to_json(indent=2))
            print(f"📝 Saved metadata: {metadata_path.name}")

        checkpoints_folder = model_folder / "checkpoints"
        checkpoints_folder.mkdir(exist_ok=True)
        training_folder = model_folder / "training_data"
        training_folder.mkdir(exist_ok=True)

        for _, row in versions.iterrows():
            model_url = row.get("downloadUrl")
            version_id = row.get("id")
            version_name = row.get("name", f"v{version_id}").replace('/', '_').strip()

            print(f"🔗 Checking model version {version_id}: {model_url}")

            if model_url:
                filename = f"{version_name}_{version_id}.safetensors"
                output_path = checkpoints_folder / filename

                if output_path.exists():
                    print(f"✅ Checkpoint exists: {filename}")
                else:
                    download_file(
                        model_url,
                        checkpoints_folder,
                        fallback_name=filename,
                        model_name=model_name,
                        version_id=version_id,
                        file_type="checkpoint"
                    )
            for col in row.index:
                if "training" in col.lower() and "url" in col.lower():
                    training_url = row[col]
                    if isinstance(training_url, str) and training_url.startswith("http"):
                        training_filename = os.path.basename(urlparse(training_url).path)
                        download_file(training_url, training_folder, fallback_name=training_filename, model_name=model_name, version_id=version_id, file_type="training_data")

        print(f"✅ Exported: {base_model}/{model_name}")
