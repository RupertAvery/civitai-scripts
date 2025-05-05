import os
import hashlib
import requests
import json
import argparse

# Config
BASE_URL = "https://civitai.com/api/v1"
EXTENSION = ".safetensors"

def get_sha256(file_path):
    """Compute the SHA-256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def fetch_model_version_by_hash(file_hash):
    """Query Civitai for a model version using the file hash."""
    url = f"{BASE_URL}/model-versions/by-hash/{file_hash}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"[ERROR] Failed to fetch model version for hash {file_hash} - {response.status_code}")
        return None

def fetch_model_details(model_id):
    """Query Civitai for model details using the model ID."""
    url = f"{BASE_URL}/models/{model_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"[ERROR] Failed to fetch model for ID {model_id} - {response.status_code}")
        return None

def check_json_exists(file_path):
    json_path = os.path.splitext(file_path)[0] + ".json"
    return os.path.exists(json_path)

def save_model_info(file_path, data):
    """Save JSON data next to the original file."""
    json_path = os.path.splitext(file_path)[0] + ".json"
    try:
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"[INFO] Saved model info to {json_path}")
    except Exception as e:
        print(f"[ERROR] Failed to save JSON for {file_path} - {e}")

def main(search_dir: str, skip: bool):
    for root, dirs, files in os.walk(search_dir):
        for file in files:
            if file.endswith(EXTENSION):
                file_path = os.path.join(root, file)
                json_exists = check_json_exists(file_path)
                if skip and json_exists:
                    print(f"\n[SKIPPING] {file_path}")
                    continue

                print(f"\n[PROCESSING] {file_path}")

                file_hash = get_sha256(file_path)
                print(f"[HASH] {file_hash}")

                model_version = fetch_model_version_by_hash(file_hash)
                if not model_version:
                    continue

                model_id = model_version.get("modelId")
                model_info = {
                    "modelVersion": model_version
                }

                if model_id:
                    model_details = fetch_model_details(model_id)
                    if model_details:
                        model_info["model"] = model_details

                save_model_info(file_path, model_info)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scan .safetensors files and fetch Civitai metadata.")
    parser.add_argument("search_dir", help="Path to directory containing .safetensors files")
    parser.add_argument('--skip',
                    action='store_true',
                    default = False,
                    dest='skip')
    args = parser.parse_args()

    main(args.search_dir, args.skip)
