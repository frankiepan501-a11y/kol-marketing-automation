"""Public error diagnostics: never include raw messages, URLs or metadata."""
import re
from .clients import ApiError

def safe_failure(error: Exception) -> dict[str, str]:
    result = {'error_type': type(error).__name__}
    if isinstance(error, ApiError):
        result['error_service'] = error.service if error.service in {'feishu', 'youtube', 'http', 'network'} else 'unknown'
        code = str(error.code)
        result['error_code'] = code if re.fullmatch(r'[A-Za-z0-9_]{1,32}', code) else 'unknown'
    return result
