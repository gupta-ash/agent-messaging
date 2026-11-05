"""Broker dashboard package (read-only).

Re-exports the single APIRouter so the broker mount is one line:

    from dashboard import router as dashboard_router
    app.include_router(dashboard_router)

The package never imports server.py (avoids a circular import, since server.py
imports this package) and only ever opens coordination.db in read-only mode.
"""

from .api import router

__all__ = ["router"]
