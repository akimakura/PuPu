"""Пример сервиса."""

from fastapi import APIRouter

from .composite import router as composites_router

router = APIRouter()
router.include_router(router=composites_router)
