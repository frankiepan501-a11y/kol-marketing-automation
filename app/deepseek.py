"""DeepSeek API"""
import httpx, json
from . import config


class DeepSeekTerminalError(RuntimeError):
    """认证或余额故障；继续重试只会放大无效请求。"""

    def __init__(self, status_code: int):
        self.status_code = int(status_code)
        super().__init__(f"DeepSeek terminal provider failure: HTTP {self.status_code}")


def is_terminal_error(exc: Exception) -> bool:
    return isinstance(exc, DeepSeekTerminalError)


async def chat_json(prompt: str, max_tokens: int = 400, temperature: float = 0.1):
    api_key = config.KOL_DEEPSEEK_API_KEY.strip()
    if not api_key:
        raise RuntimeError("missing KOL_DEEPSEEK_API_KEY")
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
            headers={"Authorization": f"Bearer {api_key}"},
        )
        if r.status_code in {401, 402}:
            raise DeepSeekTerminalError(r.status_code)
        r.raise_for_status()
        d = r.json()
        return json.loads(d["choices"][0]["message"]["content"])
