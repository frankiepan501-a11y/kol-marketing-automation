"""DeepSeek API"""
import httpx, json
from . import config


async def chat_json(prompt: str, max_tokens: int = 400, temperature: float = 0.1):
    async with httpx.AsyncClient(timeout=45.0) as cli:
        r = await cli.post(
            "https://api.deepseek.com/chat/completions",
            json={
                "model": "deepseek-chat",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": temperature,
                "max_tokens": max_tokens,
                "response_format": {"type": "json_object"},
            },
            headers={"Authorization": f"Bearer {config.DEEPSEEK_API_KEY}"},
        )
        r.raise_for_status()
        d = r.json()
        return json.loads(d["choices"][0]["message"]["content"])
