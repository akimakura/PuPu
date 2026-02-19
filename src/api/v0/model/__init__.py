"""Сервис моделей."""

from fastapi import APIRouter

from src.api.v0.const import MODEL_URL, TENANT_NAME_URL
from src.api.v0.model.model import router as model_router

router = APIRouter(prefix=TENANT_NAME_URL + MODEL_URL)
router.include_router(router=model_router)
