from fastapi import FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException


def setup_error_handlers(app: FastAPI) -> None:
    """
    Настройка обработчиков ошибок приложения FastAPI.

    Args:
        app (FastAPI): Экземпляр приложения FastAPI.

    """

    @app.exception_handler(StarletteHTTPException)
    async def custom_http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        """
        Обрабатывает стандартные HTTP исключения (например, ошибки 4xx и 5xx).

        Возвращает стандартный JSON Response с подробностью сообщения об ошибке.

        Args:
            request (Request): Объект запроса FastAPI.
            exc (StarletteHTTPException): Исключение HTTP.

        Returns:
            JSONResponse: Ответ в формате JSON с деталями ошибки.
        """
        return JSONResponse(status_code=exc.status_code, content={"detail": {"msg": exc.detail}})

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
        """
        Обрабатывает исключение валидации тела запроса.

        Передаёт обработку стандартному обработчику FastAPI для корректной обработки ошибки.

        Args:
            request (Request): Объект запроса FastAPI.
            exc (RequestValidationError): Исключение валидации запроса.

        Returns:
            JSONResponse: Стандартный обработанный ответ от FastAPI с ошибкой валидации.
        """
        return await request_validation_exception_handler(request, exc)
