"""Сервис DSO."""

from fastapi import APIRouter

from src.api.v1.const import DATABASE_URL, TENANT_NAME_URL
from src.api.v1.database.database import router as dso_router

router = APIRouter(prefix=TENANT_NAME_URL + DATABASE_URL)
router.include_router(router=dso_router)
