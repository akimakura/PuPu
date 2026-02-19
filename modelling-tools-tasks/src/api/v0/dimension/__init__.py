"""Пример сервиса."""

from fastapi import APIRouter

from .dimension import router as dimension_router

router = APIRouter()
router.include_router(router=dimension_router)
