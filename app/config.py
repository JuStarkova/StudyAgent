import os
from pathlib import Path

import yaml
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / "data"))
UPLOADS_DIR = DATA_DIR / "uploads"
CORPUS_DIR = DATA_DIR / "corpus"
DB_PATH = DATA_DIR / "app.db"

load_dotenv(BASE_DIR / ".env")

for d in (DATA_DIR, UPLOADS_DIR, CORPUS_DIR):
    d.mkdir(parents=True, exist_ok=True)


def load_models_config() -> dict:
    path = BASE_DIR / "models.yaml"
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    cfg.setdefault("models", [])
    return cfg


def get_model_config(model_id: str | None = None) -> dict:
    cfg = load_models_config()
    models = {m["id"]: m for m in cfg["models"]}
    mid = model_id or cfg.get("default")
    if mid not in models:
        if not models:
            raise RuntimeError("В models.yaml не задано ни одной модели")
        mid = cfg.get("default") or next(iter(models))
        if mid not in models:
            mid = next(iter(models))
    return models[mid]
