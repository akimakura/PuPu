"""Версия API v0."""

from fastapi import APIRouter

from src.api.v0 import aor, composite, data_storage, database, dimension, hierarchy, measure, model, tenant
from src.api.v0.const import V0_PREFIX_URL

router = APIRouter(prefix=V0_PREFIX_URL)
router.include_router(router=aor.router)
router.include_router(router=tenant.router)
router.include_router(router=database.router)
router.include_router(router=model.router)
router.include_router(router=dimension.router)
router.include_router(router=measure.router)
router.include_router(router=hierarchy.router)
router.include_router(router=data_storage.router)
router.include_router(router=composite.router)
