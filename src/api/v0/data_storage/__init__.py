"""Сервис DSO."""

from fastapi import APIRouter

from src.api.v0.const import TENANT_NAME_URL
from src.api.v0.data_storage.data_storage import router as dso_router

router = APIRouter(prefix=TENANT_NAME_URL)
router.include_router(router=dso_router)
