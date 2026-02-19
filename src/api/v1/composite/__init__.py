"""Сервис DSO."""

from fastapi import APIRouter

from src.api.v1.composite.composite import router as composite_router
from src.api.v1.const import TENANT_NAME_URL

router = APIRouter(prefix=TENANT_NAME_URL)
router.include_router(router=composite_router)
