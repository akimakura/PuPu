"""Сервис иерархий."""

from fastapi import APIRouter

from src.api.v0.const import TENANT_URL
from src.api.v0.tenant.tenant import router as tenant_router

router = APIRouter(prefix=TENANT_URL)
router.include_router(router=tenant_router)
