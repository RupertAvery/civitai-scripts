# Civitai Scripts

Here are a bunch of scripts that might be useful to Civitai users:

## Prerequisites

* Python 3.10+
* requests package

```
pip install requests pandas tqdm
```

# pandas and tqdm dependencies required for the downloader

## Download Metadata for your models

`download_your_model_metadata.py` downloads metadata based on your models. 

Just point the script at the folder or network folder where your models are.

This computes the SHA-256 of your model and uses it to lookup the model in Civitai. Useful when you don't know the actual name of the model.

The data downloaded is the raw api response in json format. It will be downloaded next to your model with the same filename and .json extension.

Works with LORAs and Checkpoints. .pt should work too if you change the extension in the script.

If you are already using Stability Matrix or something similar to download metadata from Civitai, then this isn't really useful.

The purpose of this is to quickly grab the metadata for all your LORAs, which includes any trigger words.

Usage:

```
python download_your_model_metadata.py <path to safetensors>
```

## Download all searchable model metadata from Civitai

`download_civitai_models_metadata.py` will download all metadata of a specific model type using the Civitai API.

Civitai has begun blocking results of celebrities and other stuff from search results, but it looks like the models and metadata are still there.

Usage:

```
python download_civitai_models_metadata.py <type> [--apikey APIKEY] [--cursor CURSOR]
```

Parameters:

* `type` - Checkpoint, TextualInversion, Hypernetwork, AestheticGradient, LORA, Controlnet, Poses
* `apikey` - Your Civitai API Key. Not actually needed.
* `cursor` - After each download, the API returns a "cursor" to the next page. If an error occurs, copy the last cursor that failed and use it here to continue downloading.
  
e.g. if it fails while downloading LORAs on this page:

`https://civitai.com/api/v1/models?limit=100&page=1&types=LORA&nsfw=true&token=fe7049c3bb1d65cc1bc562a0b9723e5b&cursor=75%7C367%7C474212`

Your command will be:

```
python download_civitai_models_metadata.py LORA --apikey 123456789abcdef --cursor 75%7C367%7C474212
```

## Convert JSON Metadata into a SQLite database

`convert_json_to_sqlite.py` will parse all JSON files in the folder and create or update a SQLite database named `models.db`

The script will not create duplicates so it should be fine to re-run it on existing data.


