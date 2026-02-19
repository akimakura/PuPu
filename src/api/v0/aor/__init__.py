"""Сервис моделей."""

from fastapi import APIRouter

from src.api.v0.aor.aor import router as aor_router

router = APIRouter()
router.include_router(router=aor_router)
