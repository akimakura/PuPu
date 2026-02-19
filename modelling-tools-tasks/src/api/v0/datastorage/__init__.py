"""Пример сервиса."""

from fastapi import APIRouter

from .datastorage import router as datastorages_router

router = APIRouter()
router.include_router(router=datastorages_router)
