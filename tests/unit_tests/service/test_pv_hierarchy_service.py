"""
Тесты для HierarchyPvdService — создание, обновление и удаление иерархий в PVD.
"""

from contextlib import nullcontext as does_not_raise
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import NoResultFound
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.database import Database
from src.db.dimension import Dimension, HierarchyBaseDimension, PVDctionary
from src.db.hierarchy import HierarchyLabel, HierarchyMeta, HierarchyStructureType, TimeDependencyType
from src.db.model import Model
from src.models.hierarchy import HierarchyPvdCreateRequest
from src.models.label import LabelType
from src.models.enum import InformationCategoryEnum
from src.repository.hierarchy import HierarchyRepository

from src.service.pv_hierarchy import HierarchyPvdService


def _make_database(name: str = "test_database1") -> Database:
    """Создаёт тестовую базу данных."""
    return Database(
        version=1,
        timestamp=datetime(2023, 1, 1),
        name=name,
        tenant_id="tenant1",
        type="CLICKHOUSE",
        db_name="ch1",
        default_cluster_name="test_cluster",
    )


def _make_model(name: str = "test_model1", database: Database | None = None) -> Model:
    """Создаёт тестовую модель."""
    return Model(
        version=1,
        timestamp=datetime(2023, 1, 1),
        name=name,
        schema_name=f"{name}_schema",
        tenant_id="tenant1",
        dimension_tech_fields=False,
        database=database or _make_database(),
    )


def _make_pv_dictionary(
    object_name: str = "dfTestDim",
    domain_name: str = "TestDomain",
    domain_label: str = "TestDomainLabel",
) -> PVDctionary:
    """Создаёт тестовый PV Dictionary для измерения."""
    return PVDctionary(
        object_id=0,
        object_name=object_name,
        object_type="DICTIONARY",
        domain_name=domain_name,
        domain_label=domain_label,
        status="ACTIVE",
    )


def _make_dimension(
    name: str = "test_dim",
    model: Model | None = None,
    pv_dictionary: PVDctionary | None = None,
) -> Dimension:
    """Создаёт тестовое измерение."""
    return Dimension(
        version=1,
        timestamp=datetime(2023, 1, 1),
        name=name,
        information_category=InformationCategoryEnum.K3,
        type="STRING",
        precision=10,
        auth_relevant=False,
        texts_time_dependency=False,
        texts_language_dependency=False,
        tenant_id="tenant1",
        attributes_time_dependency=False,
        case_sensitive=False,
        models=[model] if model else [],
        pv_dictionary=pv_dictionary,
    )


def _make_hierarchy(
    name: str = "test_hierarchy",
    is_versioned: bool = False,
    is_time_dependent: bool = True,
    time_dependency_type: str | None = TimeDependencyType.WHOLE,
    pv_dictionary: PVDctionary | None = None,
    labels: list | None = None,
    models: list | None = None,
) -> HierarchyMeta:
    """Создаёт тестовую иерархию."""
    return HierarchyMeta(
        version=1,
        timestamp=datetime(2023, 1, 1),
        name=name,
        default_expansion=3,
        structure_type=HierarchyStructureType.MIXED.value,
        time_dependency_type=time_dependency_type,
        is_time_dependent=is_time_dependent,
        is_versioned=is_versioned,
        input_on_nodes=False,
        default_hierarchy=False,
        data_storage_versions="hier_versions",
        data_storage_text_versions="hier_text_versions",
        data_storage_nodes="hier_nodes",
        data_storage_text_nodes="hier_text_nodes",
        labels=labels or [],
        models=models or [],
        pv_dictionary=pv_dictionary,
    )


def _make_hierarchy_label(
    language: str = "ru-ru",
    label_type: str = LabelType.SHORT,
    text: str = "Тестовая иерархия",
) -> HierarchyLabel:
    """Создаёт тестовую метку иерархии."""
    return HierarchyLabel(
        version=1,
        timestamp=datetime(2023, 1, 1),
        language=language,
        type=label_type,
        text=text,
    )


def _build_service(session: AsyncSession, dimension: Dimension | None = None) -> HierarchyPvdService:
    """Собирает HierarchyPvdService с реальным репозиторием и замоканным DimensionService."""
    hierarchy_repo = HierarchyRepository.get_by_session(session)
    dimension_service = MagicMock()
    if dimension is not None:
        dimension_service.configure_mock(
            get_dimension_orm_model=AsyncMock(return_value=dimension),
        )
    return HierarchyPvdService(
        hierarchy_repo=hierarchy_repo,
        dimension_service=dimension_service,
    )

@pytest.mark.asyncio
class TestHierarchyPvdServiceCreate:
    """Тесты создания иерархии в PVD."""

    @patch("src.service.pv_hierarchy.ClientPVDictionaries")
    async def test_create_hierarchy_in_pvd_success(
        self,
        mock_client_cls: MagicMock,
        mocked_session: AsyncSession,
    ) -> None:
        """Успешное создание иерархии в PVD."""
        mock_client = AsyncMock()
        mock_client.create_hierarchy.return_value = {}
        mock_client_cls.return_value = mock_client

        model = _make_model()
        pv_dict_dim = _make_pv_dictionary(object_name="dfTestDim")
        dimension = _make_dimension(name="test_dim", model=model, pv_dictionary=pv_dict_dim)
        labels = [
            _make_hierarchy_label("ru-ru", LabelType.SHORT, "Оргструктура"),
            _make_hierarchy_label("ru-ru", LabelType.LONG, "Описание иерархии"),
        ]
        hierarchy = _make_hierarchy(
            name="test_hier",
            labels=labels,
            models=[model],
        )

        mocked_session.add(model.database)
        mocked_session.add(model)
        mocked_session.add(pv_dict_dim)
        mocked_session.add(dimension)
        mocked_session.add(hierarchy)
        await mocked_session.flush()

        base_dim_relation = HierarchyBaseDimension(
            hierarchy_id=hierarchy.id,
            dimension_id=dimension.id,
            is_base=True,
            version=1,
            timestamp=datetime(2023, 1, 1),
        )
        mocked_session.add(base_dim_relation)
        await mocked_session.commit()

        service = _build_service(mocked_session, dimension)

        pv_request = HierarchyPvdCreateRequest(
            domain_name="TestDomain",
            domain_label="TestDomainLabel",
        )

        result = await service.create_hierarchy_in_pvd(
            tenant_id="tenant1",
            model_name="test_model1",
            dimension_name="test_dim",
            hierarchy_name="test_hier",
            pv_request=pv_request,
        )

        mock_client.create_hierarchy.assert_called_once()
        assert result is not None
        assert hierarchy.pv_dictionary is not None
        assert result.pv_dictionary is not None
        assert result.pv_dictionary.name == "dfTestDim__testHier"
        assert result.pv_dictionary.domain_name == "TestDomain"
        assert result.pv_dictionary.domain_label == "TestDomainLabel"

    @patch("src.service.pv_hierarchy.ClientPVDictionaries")
    async def test_create_hierarchy_in_pvd_already_has_pvd(
        self,
        mock_client_cls: MagicMock,
        mocked_session: AsyncSession,
    ) -> None:
        """Если иерархия уже имеет PV Dictionary — обновляется локальная запись, PVD не вызывается."""
        mock_client = AsyncMock()
        mock_client_cls.return_value = mock_client

        model = _make_model()
        pv_dict_dim = _make_pv_dictionary(object_name="dfTestDim")
        dimension = _make_dimension(name="test_dim", model=model, pv_dictionary=pv_dict_dim)
        existing_pvd = _make_pv_dictionary(object_name="dfTestDim__testHier")
        existing_pvd.object_type = "HIERARCHY"
        hierarchy = _make_hierarchy(
            name="test_hier",
            pv_dictionary=existing_pvd,
            models=[model],
        )

        mocked_session.add(model.database)
        mocked_session.add(model)
        mocked_session.add(pv_dict_dim)
        mocked_session.add(dimension)
        mocked_session.add(existing_pvd)
        mocked_session.add(hierarchy)
        await mocked_session.flush()

        base_dim_relation = HierarchyBaseDimension(
            hierarchy_id=hierarchy.id,
            dimension_id=dimension.id,
            is_base=True,
            version=1,
            timestamp=datetime(2023, 1, 1),
        )
        mocked_session.add(base_dim_relation)
        await mocked_session.commit()

        service = _build_service(mocked_session, dimension=dimension)

        result = await service.create_hierarchy_in_pvd(
            tenant_id="tenant1",
            model_name="test_model1",
            dimension_name="test_dim",
            hierarchy_name="test_hier",
        )

        assert result is not None
        mock_client.create_hierarchy.assert_not_called()

    async def test_create_hierarchy_in_pvd_not_found(
        self,
        mocked_session: AsyncSession,
    ) -> None:
        """Ошибка: иерархия не найдена."""
        service = _build_service(mocked_session)

        with pytest.raises(NoResultFound, match="не найдена"):
            await service.create_hierarchy_in_pvd(
                tenant_id="tenant1",
                model_name="test_model1",
                dimension_name="test_dim",
                hierarchy_name="nonexistent",
            )

    @patch("src.service.pv_hierarchy.ClientPVDictionaries")
    async def test_create_hierarchy_in_pvd_client_error(
        self,
        mock_client_cls: MagicMock,
        mocked_session: AsyncSession,
    ) -> None:
        """Ошибка: PVD клиент вернул ошибку."""
        mock_client = AsyncMock()
        mock_client.create_hierarchy.side_effect = Exception("PVD unavailable")
        mock_client_cls.return_value = mock_client

        model = _make_model()
        pv_dict_dim = _make_pv_dictionary(object_name="dfTestDim")
        dimension = _make_dimension(name="test_dim", model=model, pv_dictionary=pv_dict_dim)
        hierarchy = _make_hierarchy(
            name="test_hier",
            models=[model],
        )

        mocked_session.add(model.database)
        mocked_session.add(model)
        mocked_session.add(pv_dict_dim)
        mocked_session.add(dimension)
        mocked_session.add(hierarchy)
        await mocked_session.flush()

        base_dim_relation = HierarchyBaseDimension(
            hierarchy_id=hierarchy.id,
            dimension_id=dimension.id,
            is_base=True,
            version=1,
            timestamp=datetime(2023, 1, 1),
        )
        mocked_session.add(base_dim_relation)
        await mocked_session.commit()

        service = _build_service(mocked_session, dimension)

        with pytest.raises(Exception, match="PVD unavailable"):
            await service.create_hierarchy_in_pvd(
                tenant_id="tenant1",
                model_name="test_model1",
                dimension_name="test_dim",
                hierarchy_name="test_hier",
            )


@pytest.mark.asyncio
class TestHierarchyPvdServiceUpdate:
    """Тесты обновления иерархии в PVD."""

    @patch("src.service.pv_hierarchy.ClientPVDictionaries")
    async def test_update_hierarchy_in_pvd_success(
        self,
        mock_client_cls: MagicMock,
        mocked_session: AsyncSession,
    ) -> None:
        """Успешное обновление иерархии в PVD."""
        mock_client = AsyncMock()
        mock_client.update_hierarchy.return_value = {}
        mock_client_cls.return_value = mock_client

        model = _make_model()
        pv_dict_dim = _make_pv_dictionary(object_name="dfTestDim")
        dimension = _make_dimension(name="test_dim", model=model, pv_dictionary=pv_dict_dim)

        hier_pvd = _make_pv_dictionary(object_name="dfTestDim__testHier")
        hier_pvd.object_type = "HIERARCHY"

        labels = [
            _make_hierarchy_label("ru-ru", LabelType.SHORT, "Оргструктура"),
            _make_hierarchy_label("ru-ru", LabelType.LONG, "Описание иерархии"),
        ]
        hierarchy = _make_hierarchy(
            name="test_hier",
            labels=labels,
            models=[model],
            pv_dictionary=hier_pvd,
        )

        mocked_session.add(model.database)
        mocked_session.add(model)
        mocked_session.add(pv_dict_dim)
        mocked_session.add(dimension)
        mocked_session.add(hier_pvd)
        mocked_session.add(hierarchy)
        await mocked_session.flush()

        base_dim_relation = HierarchyBaseDimension(
            hierarchy_id=hierarchy.id,
            dimension_id=dimension.id,
            is_base=True,
            version=1,
            timestamp=datetime(2023, 1, 1),
        )
        mocked_session.add(base_dim_relation)
        await mocked_session.commit()

        service = _build_service(mocked_session)

        result = await service.update_hierarchy_in_pvd(
            tenant_id="tenant1",
            model_name="test_model1",
            dimension_name="test_dim",
            hierarchy_name="test_hier",
        )

        mock_client.update_hierarchy.assert_called_once()
        assert result is not None

    @patch("src.service.pv_hierarchy.ClientPVDictionaries")
    async def test_update_hierarchy_in_pvd_no_pvd(
        self,
        mock_client_cls: MagicMock,
        mocked_session: AsyncSession,
    ) -> None:
        """Если иерархия не имеет PV Dictionary — автоматически создаёт её в PVD."""
        mock_client = AsyncMock()
        mock_client.create_hierarchy.return_value = {}
        mock_client_cls.return_value = mock_client

        model = _make_model()
        pv_dict_dim = _make_pv_dictionary(object_name="dfTestDim")
        dimension = _make_dimension(name="test_dim", model=model, pv_dictionary=pv_dict_dim)
        hierarchy = _make_hierarchy(
            name="test_hier",
            models=[model],
        )

        mocked_session.add(model.database)
        mocked_session.add(model)
        mocked_session.add(pv_dict_dim)
        mocked_session.add(dimension)
        mocked_session.add(hierarchy)
        await mocked_session.flush()

        base_dim_relation = HierarchyBaseDimension(
            hierarchy_id=hierarchy.id,
            dimension_id=dimension.id,
            is_base=True,
            version=1,
            timestamp=datetime(2023, 1, 1),
        )
        mocked_session.add(base_dim_relation)
        await mocked_session.commit()

        service = _build_service(mocked_session, dimension=dimension)

        result = await service.update_hierarchy_in_pvd(
            tenant_id="tenant1",
            model_name="test_model1",
            dimension_name="test_dim",
            hierarchy_name="test_hier",
        )

        assert result is not None
        mock_client.create_hierarchy.assert_called_once()

    async def test_update_hierarchy_in_pvd_not_found(
        self,
        mocked_session: AsyncSession,
    ) -> None:
        """Ошибка: иерархия не найдена."""
        service = _build_service(mocked_session)

        with pytest.raises(NoResultFound, match="не найдена"):
            await service.update_hierarchy_in_pvd(
                tenant_id="tenant1",
                model_name="test_model1",
                dimension_name="test_dim",
                hierarchy_name="nonexistent",
            )

    @patch("src.service.pv_hierarchy.ClientPVDictionaries")
    async def test_update_hierarchy_in_pvd_updates_domain(
        self,
        mock_client_cls: MagicMock,
        mocked_session: AsyncSession,
    ) -> None:
        """Обновление domainName и domainLabel через pv_request."""
        mock_client = AsyncMock()
        mock_client.update_hierarchy.return_value = {}
        mock_client_cls.return_value = mock_client

        model = _make_model()
        pv_dict_dim = _make_pv_dictionary(object_name="dfTestDim")
        dimension = _make_dimension(name="test_dim", model=model, pv_dictionary=pv_dict_dim)

        hier_pvd = _make_pv_dictionary(
            object_name="dfTestDim__testHier",
            domain_name="OldDomain",
            domain_label="OldLabel",
        )
        hier_pvd.object_type = "HIERARCHY"

        hierarchy = _make_hierarchy(
            name="test_hier",
            models=[model],
            pv_dictionary=hier_pvd,
        )

        mocked_session.add(model.database)
        mocked_session.add(model)
        mocked_session.add(pv_dict_dim)
        mocked_session.add(dimension)
        mocked_session.add(hier_pvd)
        mocked_session.add(hierarchy)
        await mocked_session.flush()

        base_dim_relation = HierarchyBaseDimension(
            hierarchy_id=hierarchy.id,
            dimension_id=dimension.id,
            is_base=True,
            version=1,
            timestamp=datetime(2023, 1, 1),
        )
        mocked_session.add(base_dim_relation)
        await mocked_session.commit()

        service = _build_service(mocked_session)

        pv_request = HierarchyPvdCreateRequest(
            domain_name="NewDomain",
            domain_label="NewLabel",
        )

        await service.update_hierarchy_in_pvd(
            tenant_id="tenant1",
            model_name="test_model1",
            dimension_name="test_dim",
            hierarchy_name="test_hier",
            pv_request=pv_request,
        )

        assert hier_pvd.domain_name == "NewDomain"
        assert hier_pvd.domain_label == "NewLabel"


@pytest.mark.asyncio
class TestHierarchyPvdServiceDelete:
    """Тесты удаления иерархии из PVD."""

    @patch("src.service.pv_hierarchy.ClientPVDictionaries")
    async def test_delete_hierarchy_from_pvd_success(
        self,
        mock_client_cls: MagicMock,
        mocked_session: AsyncSession,
    ) -> None:
        """Успешное удаление иерархии из PVD."""
        mock_client = AsyncMock()
        mock_client.delete_hierarchy.return_value = None
        mock_client_cls.return_value = mock_client

        model = _make_model()
        pv_dict_dim = _make_pv_dictionary(object_name="dfTestDim")
        dimension = _make_dimension(name="test_dim", model=model, pv_dictionary=pv_dict_dim)
        hier_pvd = _make_pv_dictionary(object_name="dfTestDim__testHier")
        hier_pvd.object_type = "HIERARCHY"
        hierarchy = _make_hierarchy(
            name="test_hier",
            models=[model],
            pv_dictionary=hier_pvd,
        )

        mocked_session.add(model.database)
        mocked_session.add(model)
        mocked_session.add(pv_dict_dim)
        mocked_session.add(dimension)
        mocked_session.add(hier_pvd)
        mocked_session.add(hierarchy)
        await mocked_session.flush()

        base_dim_relation = HierarchyBaseDimension(
            hierarchy_id=hierarchy.id,
            dimension_id=dimension.id,
            is_base=True,
            version=1,
            timestamp=datetime(2023, 1, 1),
        )
        mocked_session.add(base_dim_relation)
        await mocked_session.commit()

        service = _build_service(mocked_session)

        await service.delete_hierarchy_from_pvd(
            tenant_id="tenant1",
            model_name="test_model1",
            dimension_name="test_dim",
            hierarchy_name="test_hier",
        )

        mock_client.delete_hierarchy.assert_called_once_with("dfTestDim__testHier")
        await mocked_session.refresh(hierarchy)
        assert hierarchy.pv_dictionary_id is None
        assert hierarchy.pv_dictionary is None

    @patch("src.service.pv_hierarchy.ClientPVDictionaries")
    async def test_delete_hierarchy_from_pvd_no_pvd(
        self,
        mock_client_cls: MagicMock,
        mocked_session: AsyncSession,
    ) -> None:
        """Ошибка: иерархия не имеет PV Dictionary."""
        model = _make_model()
        pv_dict_dim = _make_pv_dictionary(object_name="dfTestDim")
        dimension = _make_dimension(name="test_dim", model=model, pv_dictionary=pv_dict_dim)
        hierarchy = _make_hierarchy(
            name="test_hier",
            models=[model],
        )

        mocked_session.add(model.database)
        mocked_session.add(model)
        mocked_session.add(pv_dict_dim)
        mocked_session.add(dimension)
        mocked_session.add(hierarchy)
        await mocked_session.flush()

        base_dim_relation = HierarchyBaseDimension(
            hierarchy_id=hierarchy.id,
            dimension_id=dimension.id,
            is_base=True,
            version=1,
            timestamp=datetime(2023, 1, 1),
        )
        mocked_session.add(base_dim_relation)
        await mocked_session.commit()

        service = _build_service(mocked_session)

        with pytest.raises(ValueError, match="не имеет PV Dictionary"):
            await service.delete_hierarchy_from_pvd(
                tenant_id="tenant1",
                model_name="test_model1",
                dimension_name="test_dim",
                hierarchy_name="test_hier",
            )

    async def test_delete_hierarchy_from_pvd_not_found(
        self,
        mocked_session: AsyncSession,
    ) -> None:
        """Ошибка: иерархия не найдена."""
        service = _build_service(mocked_session)

        with pytest.raises(NoResultFound, match="не найдена"):
            await service.delete_hierarchy_from_pvd(
                tenant_id="tenant1",
                model_name="test_model1",
                dimension_name="test_dim",
                hierarchy_name="nonexistent",
            )

    @patch("src.service.pv_hierarchy.ClientPVDictionaries")
    async def test_delete_hierarchy_from_pvd_client_error(
        self,
        mock_client_cls: MagicMock,
        mocked_session: AsyncSession,
    ) -> None:
        """Ошибка: PVD клиент вернул ошибку — удаление не выполняется."""
        mock_client = AsyncMock()
        mock_client.delete_hierarchy.side_effect = Exception("PVD delete failed")
        mock_client_cls.return_value = mock_client

        model = _make_model()
        pv_dict_dim = _make_pv_dictionary(object_name="dfTestDim")
        dimension = _make_dimension(name="test_dim", model=model, pv_dictionary=pv_dict_dim)
        hier_pvd = _make_pv_dictionary(object_name="dfTestDim__testHier")
        hier_pvd.object_type = "HIERARCHY"
        hierarchy = _make_hierarchy(
            name="test_hier",
            models=[model],
            pv_dictionary=hier_pvd,
        )

        mocked_session.add(model.database)
        mocked_session.add(model)
        mocked_session.add(pv_dict_dim)
        mocked_session.add(dimension)
        mocked_session.add(hier_pvd)
        mocked_session.add(hierarchy)
        await mocked_session.flush()

        base_dim_relation = HierarchyBaseDimension(
            hierarchy_id=hierarchy.id,
            dimension_id=dimension.id,
            is_base=True,
            version=1,
            timestamp=datetime(2023, 1, 1),
        )
        mocked_session.add(base_dim_relation)
        await mocked_session.commit()

        service = _build_service(mocked_session)

        with pytest.raises(Exception, match="PVD delete failed"):
            await service.delete_hierarchy_from_pvd(
                tenant_id="tenant1",
                model_name="test_model1",
                dimension_name="test_dim",
                hierarchy_name="test_hier",
            )


@pytest.mark.asyncio
class TestExtractLabelsForPvd:
    """Тесты вспомогательного метода _extract_labels_for_pvd."""

    @pytest.mark.parametrize(
        ("labels", "expected_display", "expected_desc"),
        [
            ([], None, None),
            (
                [
                    _make_hierarchy_label("ru-ru", LabelType.SHORT, "Короткий"),
                    _make_hierarchy_label("ru-ru", LabelType.LONG, "Длинный"),
                ],
                "Короткий",
                "Длинный",
            ),
            (
                [_make_hierarchy_label("en-us", LabelType.SHORT, "Short")],
                "Short",
                None,
            ),
            (
                [
                    _make_hierarchy_label("ru-ru", LabelType.SHORT, "Рус"),
                    _make_hierarchy_label("en-us", LabelType.SHORT, "Eng"),
                ],
                "Рус",
                None,
            ),
        ],
    )
    def test_extract_labels(
        self,
        labels: list[HierarchyLabel],
        expected_display: str | None,
        expected_desc: str | None,
    ) -> None:
        """Проверяет извлечение displayName и description из labels."""
        display, desc = HierarchyPvdService._extract_labels_for_pvd(labels)
        assert display == expected_display
        assert desc == expected_desc
