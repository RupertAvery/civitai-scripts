import requests
import os
import time
import urllib.parse
import argparse

def sanitize_filename(s):
    return "".join(c if c.isalnum() or c in "-_." else "_" for c in s)

def fetch_models(types, cursor, api_key):
    base_url = "https://civitai.com/api/v1/models"
    type_param = types

    params = {
        "limit": 100,
        "page": 1,
        "types": type_param,
        "nsfw": True
    }

    if api_key is not None:
        params["token"] = api_key

    if cursor is not None:
        params["cursor"] = cursor.replace('%7C', '|')
    else:
        cursor = 'start'

    while True:
        # Build URL
        url = base_url + "?" + urllib.parse.urlencode([(k, str(v).lower() if isinstance(v, bool) else v) for k, v in params.items()])
        print(f"📥 Fetching: {url}")
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"❌ Error fetching or parsing {url}: {e}")
            break

        # Save JSON
        safe_types = sanitize_filename(type_param)
        cursor_id = sanitize_filename(cursor)
        filename = f"models-{safe_types}-{cursor_id}.json"
        with open(filename, "w", encoding="utf-8") as f:
            f.write(response.text)
        print(f"✅ Saved: {filename}")

        # Check for next page
        metadata = data.get("metadata")
        if not metadata or not metadata.get("nextPage"):
            print("🚫 No more pages.")
            break

        # Prepare for next iteration
        next_page_url = metadata["nextPage"]
        parsed = urllib.parse.urlparse(next_page_url)
        params = dict(urllib.parse.parse_qsl(parsed.query))
        cursor = metadata.get("nextCursor", "done")

        # Optional: polite delay
        time.sleep(0.5)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch Civitai metadata and save to JSON")
    parser.add_argument("type", help="Checkpoint, TextualInversion, Hypernetwork, AestheticGradient, LORA, Controlnet, Poses", default = 'Checkpoint')
    parser.add_argument("--apikey", help="Your Civitai API Key")
    parser.add_argument('--cursor', help="Set to the last value of cursor if an error occurs", default=None)
    args = parser.parse_args()    

    fetch_models(args.type, args.cursor, args.apikey)
