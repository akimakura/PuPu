"""Версия API v1."""

from fastapi import APIRouter

from src.api.v1 import aor, composite, data_storage, database, dimension, measure, model, tenant
from src.api.v1.const import V1_PREFIX_URL

router = APIRouter(prefix=V1_PREFIX_URL)
router.include_router(router=aor.router)
router.include_router(router=tenant.router)
router.include_router(router=database.router)
router.include_router(router=model.router)
router.include_router(router=dimension.router)
router.include_router(router=measure.router)
router.include_router(router=data_storage.router)
router.include_router(router=composite.router)
