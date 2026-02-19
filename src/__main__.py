"""
Модуль для локального запуска сервиса Semantic Layer.
"""

import argparse
import sys

import uvicorn

from src.config import settings
from src.on_startup import run_migrations

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Запуск миграций или приложения")
    parser.add_argument("-m", action="append", help="Модель для миграции")
    parser.add_argument("-cd", action="store_true", help="Удалить все композиты.")
    parser.add_argument("-cc", action="store_true", help="Включить миграцию композитов")
    parser.add_argument("-du", action="store_true", help="Включить миграцию хранилищ через обновление")
    parser.add_argument("-dc", action="store_true", help="Включить миграцию хранилищ через пересоздание")
    parser.add_argument("-wc", action="store_true", help="Удалять ли колонки")
    parser.add_argument("-wd", action="store_true", help="Включить пересоздание непустых сущностей.")
    parser.add_argument("-rd", action="store_true", help="Включить пересоздание словарей.")
    parser.add_argument("-rs", action="store_true", help="Включить выполнение raw sql.")
    args = parser.parse_args()
    if not args.m:
        ssl_ca_certs = settings.UVICORN_PATH_TO_CA_CERT
        ssl_keyfile = settings.UVICORN_PATH_TO_CLIENT_CERT_KEY
        ssl_certfile = settings.UVICORN_PATH_TO_CLIENT_CERT
        ssl_keyfile_password = settings.UVICORN_CERT_PASSWORD
        uvicorn.run(
            "src.main:create_app",
            host=settings.BIND_IP,
            port=settings.BIND_PORT,
            workers=settings.UVICORN_WORKERS_COUNT,
            ssl_ca_certs=str(ssl_ca_certs) if ssl_ca_certs is not None else None,
            ssl_keyfile=str(ssl_keyfile) if ssl_keyfile is not None else None,
            ssl_certfile=str(ssl_certfile) if ssl_certfile is not None else None,
            ssl_keyfile_password=str(ssl_keyfile_password) if ssl_keyfile_password is not None else None,
            timeout_graceful_shutdown=settings.UVICORN_TIMEOUT_GRACEFULL_SHUTDOWN,
        )
    else:
        run_migrations(
            args.m,
            args.du,
            args.dc,
            args.wc,
            args.cc,
            args.cd,
            args.rd,
            args.wd,
            args.rs,
        )
        sys.exit(0)
