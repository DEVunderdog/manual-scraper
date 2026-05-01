"""
Shim for the emergent supervisor convention (`/app/backend/server:app` on
port 8001).

The real FastAPI application lives at :mod:`api.main`. This module
exists only because supervisor's read-only config expects a
``/app/backend/server.py`` with a top-level ``app`` symbol. Editing
supervisor isn't allowed here, so we conform on the repo side.

Importing :mod:`api.main` triggers route registration and the lifespan
context manager (Mongo connect, scraper auto-import, upstream SQS
consumer). All env vars are read by ``shared.config.settings`` from
``/app/.env`` via pydantic-settings — see ``Settings.model_config``.
"""

from __future__ import annotations

import os
import sys

# Ensure the project root is on sys.path so ``api.*`` imports resolve
# regardless of the CWD supervisor uses.
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from api.main import app  # noqa: E402,F401  re-export for `uvicorn server:app`

__all__ = ["app"]
