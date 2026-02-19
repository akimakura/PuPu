"""Сервис показателей."""

from fastapi import APIRouter

from src.api.v0.const import TENANT_NAME_URL
from src.api.v0.measure.measure import router as measure_router

router = APIRouter(prefix=TENANT_NAME_URL)
router.include_router(router=measure_router)
