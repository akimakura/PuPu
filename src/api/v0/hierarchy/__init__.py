"""Сервис иерархий."""

from fastapi import APIRouter

from src.api.v0.const import TENANT_NAME_URL
from src.api.v0.hierarchy.hierarchy import router as hierarchy_router

router = APIRouter(prefix=TENANT_NAME_URL)
router.include_router(router=hierarchy_router)
