"""FastAPI entrypoint that enables the investment assistant router.

Use with Docker env:
    ENTRY=app.invest_main:app
"""
from .main import app
from . import invest


if not any(getattr(route, "path", "").startswith("/invest/") for route in app.routes):
    app.include_router(invest.router)
