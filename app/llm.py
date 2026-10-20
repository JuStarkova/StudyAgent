import json
import os
import re

from openai import OpenAI

from .config import get_model_config


class LLMError(Exception):
    pass


def chat(messages: list[dict], model_id: str | None = None,
         max_tokens: int | None = None, temperature: float | None = None) -> str:
    cfg = get_model_config(model_id)
    api_key = os.environ.get(cfg.get("api_key_env", ""), "")
    if not api_key:
        raise LLMError(
            f"Не задан API-ключ: переменная окружения {cfg.get('api_key_env')} пуста. "
            f"Добавьте её в .env и перезапустите контейнер."
        )
    client = OpenAI(api_key=api_key, base_url=cfg["base_url"], timeout=180.0)
    try:
        resp = client.chat.completions.create(
            model=cfg["model"],
            messages=messages,
            max_tokens=max_tokens or cfg.get("max_tokens", 4096),
            temperature=temperature if temperature is not None else cfg.get("temperature", 0.3),
        )
    except Exception as e:
        raise LLMError(f"Ошибка вызова модели {cfg.get('label', cfg['model'])}: {e}") from e
    content = resp.choices[0].message.content
    return content or ""


def extract_json(text: str):
    """Достаёт JSON-объект/массив из ответа модели (поддержка ```json fences)."""
    fence = re.search(r"```(?:json)?\s*(.*?)```", text, re.DOTALL)
    candidate = fence.group(1) if fence else text
    candidate = candidate.strip()
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        pass
    for start_ch, end_ch in (("{", "}"), ("[", "]")):
        start = candidate.find(start_ch)
        end = candidate.rfind(end_ch)
        if start != -1 and end > start:
            try:
                return json.loads(candidate[start:end + 1])
            except json.JSONDecodeError:
                continue
    raise LLMError("Модель вернула ответ, который не удалось разобрать как JSON")
