"""Версия API v0.

* example - серивс-пример.
"""

from fastapi import APIRouter

from src.api.v0 import datastorage, dimension, composite

router = APIRouter(prefix="/v0")
for package_object in [datastorage, dimension, composite]:
    router.include_router(router=package_object.router)
