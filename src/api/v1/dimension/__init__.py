"""Сервис признаков."""

from fastapi import APIRouter

from src.api.v1.const import TENANT_NAME_URL
from src.api.v1.dimension.dimension import router as dimension_router

router = APIRouter(prefix=TENANT_NAME_URL)
router.include_router(router=dimension_router)
