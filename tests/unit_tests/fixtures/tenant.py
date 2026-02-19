import pytest

from src.db.tenant import Tenant, TenantLabel
from src.models.label import Label, LabelType
from src.models.tenant import (
    Tenant as TenantModel,
    TenantCreateRequest as TenantCreateRequestModel,
    TenantEditRequest as TenantEditRequestModel,
)


@pytest.fixture
def tenants() -> list[Tenant]:
    return [
        Tenant(
            name="tenant1",
            labels=[
                TenantLabel(
                    language="ru-ru",
                    type=LabelType.SHORT,
                    text="test",
                ),
                TenantLabel(
                    language="ru-ru",
                    type=LabelType.LONG,
                    text="test",
                ),
            ],
        ),
        Tenant(
            name="tenant2",
            labels=[
                TenantLabel(
                    language="ru-ru",
                    type=LabelType.SHORT,
                    text="test",
                ),
                TenantLabel(
                    language="ru-ru",
                    type=LabelType.LONG,
                    text="test",
                ),
            ],
        ),
    ]


tenant_model_list = [
    TenantModel(
        name="tenant1",
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
    TenantModel(
        name="tenant2",
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
    TenantModel(
        name="tenant3",
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
    TenantModel(
        name="tenant4",
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
]


tenant_model_create_list = [
    TenantCreateRequestModel(
        name="tenant3",
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
    TenantCreateRequestModel(
        name="tenant4",
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
]


tenant_model_update_list = [
    TenantEditRequestModel(
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
    TenantEditRequestModel(
        labels=[
            Label(
                language="ru-ru",
                type=LabelType.SHORT,
                text="test",
            ),
            Label(
                language="ru-ru",
                type=LabelType.LONG,
                text="test",
            ),
        ],
    ),
]
