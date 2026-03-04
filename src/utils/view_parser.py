from __future__ import annotations

import re
from typing import Any, Optional

import sqlglot
from py_common_lib.logger import EPMPYLogger
from sqlglot import expressions as exp

logger = EPMPYLogger(__name__)


def contains_sql_identifier(sql_text: str, identifier: str) -> bool:
    """Проверяет, что идентификатор встречается в SQL как отдельное имя."""
    if not sql_text or not identifier:
        return False
    pattern = rf"(?<![A-Za-z0-9_$]){re.escape(identifier)}(?![A-Za-z0-9_$])"
    return re.search(pattern, sql_text) is not None


def parse_view_ddl(ddl: str, view_name: str, dialect: Optional[str] = None) -> dict[str, Any]:
    """Преобразует DDL VIEW в расширенный JSON, пригодный для восстановления SQL."""
    ddl = ddl.strip().rstrip(";")
    if not ddl:
        return _empty_view_json(view_name)
    try:
        expression = sqlglot.parse_one(ddl, read=dialect)
    except Exception as exc:
        logger.warning(
            "Failed to parse VIEW DDL. view=%s dialect=%s error=%s",
            view_name,
            dialect,
            exc,
        )
        return _empty_view_json(view_name)
    view_columns = _extract_view_columns(expression, dialect)
    query_expression = expression.args.get("expression") if isinstance(expression, exp.Create) else expression
    if not isinstance(query_expression, exp.Expression):
        logger.warning(
            "VIEW DDL does not contain a valid query expression. view=%s dialect=%s",
            view_name,
            dialect,
        )
        return _empty_view_json(view_name)
    query_level = _collect_query_level(query_expression, dialect)
    extracted_expression = _extract_query_expression(query_expression)
    if extracted_expression is None:
        extracted_expression = query_expression
    query_expression = extracted_expression
    query_blocks = _collect_query_blocks(query_expression, dialect)
    if not query_blocks:
        logger.warning(
            "VIEW DDL parsed, but no SELECT blocks found. view=%s dialect=%s expr=%s query_expr=%s",
            view_name,
            dialect,
            type(expression).__name__,
            type(query_expression).__name__,
        )
        return _empty_view_json(view_name)
    columns = _collect_columns_from_block(query_blocks[0], view_columns)
    source_table = _get_source_table_name(query_blocks[0])
    where_condition = query_blocks[0].get("where")
    dependencies = _collect_dependencies(query_blocks)
    result = {
        "type": "VIEW",
        "name": view_name,
        "view_columns": view_columns,
        "queries": query_blocks,
        "source_table": source_table,
        "columns": columns,
        "where_condition": where_condition,
        "dependencies": dependencies,
    }
    if query_level:
        result["query_level"] = query_level
    return result


def build_view_sql_expression(view_json: dict[str, Any]) -> Optional[str]:
    """Восстанавливает SQL выражение SELECT/UNION из json_definition VIEW."""
    if not view_json:
        return None
    query_blocks = view_json.get("queries") or []
    if not query_blocks:
        logger.warning(
            "VIEW json_definition does not contain queries. view=%s",
            view_json.get("name"),
        )
        return None
    sql_blocks: list[str] = []
    for block in query_blocks:
        block_sql = _build_select_sql(block)
        if not block_sql:
            continue
        if sql_blocks:
            union_type = block.get("union_type")
            sql_blocks.append(f"UNION {union_type}" if union_type else "UNION")
        sql_blocks.append(block_sql)
    if not sql_blocks:
        logger.warning(
            "VIEW json_definition does not contain valid SELECT blocks. view=%s",
            view_json.get("name"),
        )
        return None
    sql_expression = " ".join(sql_blocks)
    query_level = view_json.get("query_level")
    if isinstance(query_level, dict):
        sql_expression += _build_query_level_suffix(query_level)
    return sql_expression


def _build_select_sql(query_block: dict[str, Any]) -> str:
    """Собирает SQL SELECT из одного блока запроса."""
    select_items = query_block.get("select") or []
    if not select_items:
        return ""
    select_parts: list[str] = []
    for item in select_items:
        expression_sql = item.get("expression")
        if not expression_sql:
            continue
        alias = item.get("alias")
        if alias and alias not in {"*", expression_sql}:
            select_parts.append(f"{expression_sql} AS {alias}")
        else:
            select_parts.append(expression_sql)
    if not select_parts:
        return ""
    parts = [f"SELECT {', '.join(select_parts)}"]
    from_sql = _build_from_sql(query_block.get("from") or [])
    if from_sql:
        parts.append(from_sql)
    joins_sql = _build_joins_sql(query_block.get("joins") or [])
    if joins_sql:
        parts.append(joins_sql)
    where_sql = query_block.get("where")
    if where_sql:
        parts.append(f"WHERE {where_sql}")
    group_by = query_block.get("group_by") or []
    if group_by:
        parts.append(f"GROUP BY {', '.join(group_by)}")
    having_sql = query_block.get("having")
    if having_sql:
        parts.append(f"HAVING {having_sql}")
    order_by = query_block.get("order_by") or []
    if order_by:
        parts.append(f"ORDER BY {', '.join(order_by)}")
    limit_sql = query_block.get("limit")
    if limit_sql:
        parts.append(f"LIMIT {limit_sql}")
    offset_sql = query_block.get("offset")
    if offset_sql:
        parts.append(f"OFFSET {offset_sql}")
    fetch_sql = query_block.get("fetch")
    if fetch_sql:
        parts.append(f"FETCH FIRST {fetch_sql} ROWS ONLY")
    return " ".join(parts)


def _build_from_sql(sources: list[dict[str, Any]]) -> str:
    """Собирает FROM из списка источников."""
    if not sources:
        return ""
    parts: list[str] = []
    for source in sources:
        part = _build_source_sql(source)
        if part:
            parts.append(part)
    return f"FROM {', '.join(parts)}" if parts else ""


def _build_joins_sql(joins: list[dict[str, Any]]) -> str:
    """Собирает JOIN-ы из списка источников."""
    parts: list[str] = []
    for join in joins:
        source_sql = _build_source_sql(join.get("source"))
        if not source_sql:
            continue
        join_type = join.get("type") or "INNER"
        on_condition = join.get("on")
        using_columns = join.get("using") or []
        clause = f"{join_type} JOIN {source_sql}"
        if on_condition:
            clause = f"{clause} ON {on_condition}"
        elif using_columns:
            clause = f"{clause} USING ({', '.join(using_columns)})"
        parts.append(clause)
    return " ".join(parts)


def _build_source_sql(source: Optional[dict[str, Any]]) -> str:
    """Формирует SQL для источника FROM/JOIN."""
    if not source:
        return ""
    source_type = source.get("type")
    if source_type == "table":
        name = _build_qualified_name(source)
        alias = source.get("alias")
        if alias:
            return f"{name} AS {alias}"
        return name
    if source_type == "subquery":
        query = source.get("query") or {}
        if not query:
            logger.warning("Subquery source without query definition.")
            return ""
        query_sql = _build_select_sql(query)
        if not query_sql:
            return ""
        alias = source.get("alias")
        sql = f"({query_sql})"
        return f"{sql} AS {alias}" if alias else sql
    if source_type == "expression":
        return source.get("expression") or ""
    return ""


def _build_qualified_name(source: dict[str, Any]) -> str:
    """Собирает полное имя таблицы с учетом схемы и каталога."""
    parts = [source.get("catalog"), source.get("schema"), source.get("name")]
    return ".".join([part for part in parts if part])


def _build_query_level_suffix(query_level: dict[str, Any]) -> str:
    """Формирует суффикс ORDER/LIMIT/OFFSET/FETCH для верхнего уровня запроса."""
    parts: list[str] = []
    order_by = query_level.get("order_by") or []
    if order_by:
        parts.append(f"ORDER BY {', '.join(order_by)}")
    limit_sql = query_level.get("limit")
    if limit_sql:
        parts.append(f"LIMIT {limit_sql}")
    offset_sql = query_level.get("offset")
    if offset_sql:
        parts.append(f"OFFSET {offset_sql}")
    fetch_sql = query_level.get("fetch")
    if fetch_sql:
        parts.append(f"FETCH FIRST {fetch_sql} ROWS ONLY")
    return f" {' '.join(parts)}" if parts else ""


def _extract_view_columns(expression: exp.Expression, dialect: Optional[str]) -> Optional[list[str]]:
    """Извлекает список колонок VIEW из CREATE VIEW, если он задан явно."""
    if not isinstance(expression, exp.Create):
        return None
    columns = expression.args.get("columns")
    if not columns:
        schema = expression.args.get("schema")
        if isinstance(schema, exp.Schema):
            columns = list(schema.expressions)
        else:
            schema = expression.find(exp.Schema)
            if schema is not None:
                columns = list(schema.expressions)
    if not columns:
        return None
    result: list[str] = []
    for column in columns:
        definition = _extract_view_column_definition(column, dialect)
        if definition:
            result.append(definition)
    return result or None


def _extract_view_column_definition(column: Any, dialect: Optional[str]) -> Optional[str]:
    """Возвращает строковое описание колонки из списка колонок VIEW."""
    if isinstance(column, exp.ColumnDef):
        return column.sql(dialect=dialect)
    if isinstance(column, exp.Column):
        return column.name
    if isinstance(column, exp.Identifier):
        return column.name
    if isinstance(column, exp.Expression):
        return column.sql(dialect=dialect)
    if column is None:
        return None
    return str(column)


def _extract_query_expression(expression: exp.Expression) -> Optional[exp.Expression]:
    """Возвращает выражение запроса из DDL (SELECT/UNION), если возможно."""
    union_expr = expression.find(exp.Union)
    if union_expr is not None:
        return union_expr
    if isinstance(expression, exp.Query):
        union_expr = expression.args.get("expression")
        if isinstance(union_expr, exp.Union):
            return union_expr
        return expression.this
    return expression


def _collect_query_blocks(query_expression: Optional[exp.Expression], dialect: Optional[str]) -> list[dict[str, Any]]:
    """Преобразует SELECT/UNION в список блоков запроса."""
    if query_expression is None:
        return []
    if isinstance(query_expression, exp.Union):
        blocks: list[dict[str, Any]] = []
        _walk_union(query_expression, blocks, dialect, None)
        return blocks
    select_expr = _ensure_select(query_expression)
    if select_expr is None:
        return []
    return [_build_query_block(select_expr, None, dialect)]


def _walk_union(
    union_expr: exp.Union,
    blocks: list[dict[str, Any]],
    dialect: Optional[str],
    leading_union_type: Optional[str],
) -> None:
    """Рекурсивно разворачивает цепочку UNION/UNION ALL в список блоков."""
    left = union_expr.this
    right = union_expr.expression
    if isinstance(left, exp.Union):
        _walk_union(left, blocks, dialect, leading_union_type)
    else:
        left_select = _ensure_select(left)
        if left_select is not None:
            blocks.append(_build_query_block(left_select, leading_union_type, dialect))
    union_type = "ALL" if union_expr.args.get("distinct") is False else "DISTINCT"
    if isinstance(right, exp.Union):
        _walk_union(right, blocks, dialect, union_type)
    else:
        right_select = _ensure_select(right)
        if right_select is not None:
            blocks.append(_build_query_block(right_select, union_type, dialect))


def _ensure_select(expression: exp.Expression) -> Optional[exp.Select]:
    """Возвращает SELECT из выражения, если он присутствует."""
    if isinstance(expression, exp.Select):
        return expression
    if isinstance(expression, exp.Query):
        return _ensure_select(expression.this)
    if isinstance(expression, exp.With):
        return _ensure_select(expression.this)
    if isinstance(expression, exp.Subquery):
        return _ensure_select(expression.this)
    if isinstance(expression, exp.Paren):
        return _ensure_select(expression.this)
    return None


def _build_query_block(select_expr: exp.Select, union_type: Optional[str], dialect: Optional[str]) -> dict[str, Any]:
    """Собирает структурированный JSON для одного SELECT блока."""
    return {
        "select": _collect_select_items(select_expr, dialect),
        "from": _collect_from_sources(select_expr, dialect),
        "joins": _collect_joins(select_expr, dialect),
        "where": _collect_where_condition(select_expr, dialect),
        "group_by": _collect_group_by(select_expr, dialect),
        "having": _collect_having(select_expr, dialect),
        "order_by": _collect_order_by(select_expr, dialect),
        "limit": _collect_limit(select_expr, dialect),
        "offset": _collect_offset(select_expr, dialect),
        "fetch": _collect_fetch(select_expr, dialect),
        "union_type": union_type,
    }


def _collect_select_items(select_expr: exp.Select, dialect: Optional[str]) -> list[dict[str, str]]:
    """Собирает список выражений SELECT с алиасами."""
    items: list[dict[str, str]] = []
    for item in select_expr.expressions:
        alias, expression_sql = _extract_select_item(item, dialect)
        entry = {"expression": expression_sql}
        if alias:
            entry["alias"] = alias
        items.append(entry)
    return items


def _extract_select_item(item: Any, dialect: Optional[str]) -> tuple[Optional[str], str]:
    """Возвращает алиас и SQL выражение для элемента SELECT."""
    if isinstance(item, exp.Alias):
        alias = item.alias_or_name
        expression_sql = item.this.sql(dialect=dialect)
        return alias, expression_sql
    if isinstance(item, exp.Column):
        return item.name, item.sql(dialect=dialect)
    if isinstance(item, exp.Star):
        return "*", "*"
    return None, item.sql(dialect=dialect)


def _collect_from_sources(
    select_expr: exp.Select,
    dialect: Optional[str],
    allow_reparse: bool = True,
) -> list[dict[str, Any]]:
    """Собирает источники из FROM."""
    result: list[dict[str, Any]] = []
    from_expr = select_expr.args.get("from") or select_expr.find(exp.From)
    if from_expr is not None:
        expressions = list(from_expr.expressions)
        if not expressions and from_expr.this is not None:
            expressions.append(from_expr.this)
        for from_item in expressions:
            from_item = _unwrap_parentheses(from_item)
            source = _source_from_expression(from_item, dialect)
            if source:
                result.append(source)
    if not allow_reparse or from_expr is None:
        return result
    if _needs_reparse_sources(result) and _has_join_in_from(from_expr, dialect):
        select_from_sql = _parse_select_from_from_sql(from_expr, dialect)
        if select_from_sql is not None:
            reparsed = _collect_from_sources(select_from_sql, dialect, allow_reparse=False)
            if reparsed:
                return reparsed
    return result


def _collect_joins(
    select_expr: exp.Select,
    dialect: Optional[str],
    allow_reparse: bool = True,
) -> list[dict[str, Any]]:
    """Собирает JOIN-ы с условиями и источниками."""
    result: list[dict[str, Any]] = []
    for join_expr in select_expr.args.get("joins", []):
        source = _source_from_expression(join_expr.this, dialect)
        if not source:
            continue
        join_info: dict[str, Any] = {
            "type": _normalize_join_type(join_expr),
            "source": source,
        }
        on_expr = join_expr.args.get("on")
        if on_expr is not None and on_expr.this is not None:
            join_info["on"] = on_expr.this.sql(dialect=dialect)
        using_expr = join_expr.args.get("using")
        if using_expr is not None:
            join_info["using"] = [item.sql(dialect=dialect) for item in using_expr.expressions]
        result.append(join_info)
    if result or not allow_reparse:
        return result
    from_expr = select_expr.args.get("from") or select_expr.find(exp.From)
    if from_expr is None:
        return result
    if _has_join_in_from(from_expr, dialect):
        select_from_sql = _parse_select_from_from_sql(from_expr, dialect)
        if select_from_sql is not None:
            return _collect_joins(select_from_sql, dialect, allow_reparse=False)
    return result


def _normalize_join_type(join_expr: exp.Join) -> str:
    """Возвращает тип JOIN в строковом виде."""
    kind = join_expr.args.get("kind")
    side = join_expr.args.get("side")
    parts = [part for part in [side, kind] if part]
    if not parts:
        return "INNER"
    return " ".join(parts).upper()


def _source_from_expression(expression: exp.Expression, dialect: Optional[str]) -> Optional[dict[str, Any]]:
    """Преобразует выражение источника в словарь для JSON."""
    if isinstance(expression, exp.Table):
        schema = _normalize_identifier(expression.args.get("db"), dialect)
        catalog = _normalize_identifier(expression.args.get("catalog"), dialect)
        alias = _extract_alias(expression, dialect)
        table_source = {
            "type": "table",
            "name": expression.name,
            "schema": schema,
            "catalog": catalog,
        }
        if alias:
            table_source["alias"] = alias
        return table_source
    if isinstance(expression, exp.Subquery):
        alias = _extract_alias(expression, dialect)
        select_expr = _ensure_select(expression.this)
        subquery_source: dict[str, Any] = {"type": "subquery"}
        if select_expr is not None:
            subquery_source["query"] = _build_query_block(select_expr, None, dialect)
        if alias:
            subquery_source["alias"] = alias
        return subquery_source
    return {"type": "expression", "expression": expression.sql(dialect=dialect)}


def _unwrap_parentheses(expression: exp.Expression) -> exp.Expression:
    """Снимает один уровень скобок, если это Paren."""
    if isinstance(expression, exp.Paren) and expression.this is not None:
        return expression.this
    return expression


def _needs_reparse_sources(sources: list[dict[str, Any]]) -> bool:
    """Проверяет, нужно ли повторно парсить FROM для выделения базовой таблицы."""
    if not sources:
        return True
    for source in sources:
        if source.get("type") != "subquery":
            return False
        if source.get("query") is not None:
            return False
    return True


def _has_join_in_from(from_expr: exp.From, dialect: Optional[str]) -> bool:
    """Проверяет, есть ли JOIN внутри FROM."""
    from_sql = from_expr.sql(dialect=dialect)
    return "JOIN" in from_sql.upper()


def _parse_select_from_from_sql(from_expr: exp.From, dialect: Optional[str]) -> Optional[exp.Select]:
    """Пытается построить SELECT из строки FROM для разбора JOIN."""
    from_sql = from_expr.sql(dialect=dialect).strip()
    if not from_sql:
        return None
    if _has_unsafe_from_sql(from_sql):
        return None
    if from_sql.upper().startswith("FROM "):
        from_sql = from_sql[5:].strip()
    if from_sql.startswith("(") and from_sql.endswith(")"):
        from_sql = from_sql[1:-1].strip()
    if not from_sql:
        return None
    synthetic_sql = f"SELECT * FROM {from_sql}"
    try:
        expression = sqlglot.parse_one(synthetic_sql, read=dialect)
    except Exception:
        return None
    return _ensure_select(expression)


def _has_unsafe_from_sql(from_sql: str) -> bool:
    """Определяет, можно ли безопасно перепарсить FROM через строку."""
    lowered = from_sql.lower()
    return any(token in lowered for token in ("select ", " with ", "lateral"))


def _extract_alias(expression: exp.Expression, dialect: Optional[str]) -> Optional[str]:
    """Возвращает алиас таблицы/подзапроса, если он задан."""
    alias_expr = expression.args.get("alias")
    if alias_expr is None:
        return None
    if isinstance(alias_expr, exp.TableAlias) and alias_expr.this is not None:
        return alias_expr.this.sql(dialect=dialect)
    if isinstance(alias_expr, exp.Identifier):
        return alias_expr.name
    if isinstance(alias_expr, exp.Expression):
        return alias_expr.sql(dialect=dialect)
    return str(alias_expr)


def _normalize_identifier(value: Any, dialect: Optional[str]) -> Optional[str]:
    """Нормализует sqlglot Identifier/Expression в строку для JSON."""
    if value is None:
        return None
    if isinstance(value, exp.Identifier):
        return value.name
    if isinstance(value, exp.Expression):
        return value.sql(dialect=dialect)
    return str(value)


def _collect_where_condition(select_expr: exp.Select, dialect: Optional[str]) -> Optional[str]:
    """Возвращает WHERE условие как строку SQL."""
    where_expr = select_expr.args.get("where")
    if where_expr is None or where_expr.this is None:
        return None
    return where_expr.this.sql(dialect=dialect)


def _collect_group_by(select_expr: exp.Select, dialect: Optional[str]) -> list[str]:
    """Возвращает GROUP BY выражения."""
    group_expr = select_expr.args.get("group")
    if group_expr is None:
        return []
    return [item.sql(dialect=dialect) for item in group_expr.expressions]


def _collect_having(select_expr: exp.Select, dialect: Optional[str]) -> Optional[str]:
    """Возвращает HAVING условие как строку SQL."""
    having_expr = select_expr.args.get("having")
    if having_expr is None or having_expr.this is None:
        return None
    return having_expr.this.sql(dialect=dialect)


def _collect_order_by(expression: exp.Expression, dialect: Optional[str]) -> list[str]:
    """Возвращает ORDER BY выражения."""
    order_expr = expression.args.get("order")
    if order_expr is None:
        return []
    return [item.sql(dialect=dialect) for item in order_expr.expressions]


def _collect_limit(expression: exp.Expression, dialect: Optional[str]) -> Optional[str]:
    """Возвращает LIMIT как строку SQL."""
    limit_expr = expression.args.get("limit")
    if limit_expr is None:
        return None
    limit_value = limit_expr.expression or limit_expr.args.get("expression")
    if limit_value is None:
        return None
    return limit_value.sql(dialect=dialect)


def _collect_offset(expression: exp.Expression, dialect: Optional[str]) -> Optional[str]:
    """Возвращает OFFSET как строку SQL."""
    offset_expr = expression.args.get("offset")
    if offset_expr is not None:
        value = offset_expr.expression or offset_expr.args.get("expression") or offset_expr.this
        if value is not None:
            return value.sql(dialect=dialect)
    limit_expr = expression.args.get("limit")
    if limit_expr is not None:
        offset_expr = limit_expr.args.get("offset")
        if offset_expr is not None:
            value = offset_expr.expression or offset_expr.args.get("expression") or offset_expr.this
            if value is not None:
                return value.sql(dialect=dialect)
    return None


def _collect_fetch(expression: exp.Expression, dialect: Optional[str]) -> Optional[str]:
    """Возвращает FETCH как строку SQL."""
    fetch_expr = expression.args.get("fetch")
    if fetch_expr is None:
        return None
    value = (
        fetch_expr.expression or fetch_expr.args.get("expression") or fetch_expr.args.get("count") or fetch_expr.this
    )
    if value is None:
        return None
    return value.sql(dialect=dialect)


def _collect_columns_from_block(
    query_block: dict[str, Any], view_columns: Optional[list[str]] = None
) -> list[dict[str, str]]:
    """Собирает список колонок в старом формате из блока SELECT."""
    columns: list[dict[str, str]] = []
    select_items = query_block.get("select", [])
    view_names = _normalize_view_column_names(view_columns) if view_columns else []
    use_view_names = bool(view_names) and len(view_names) == len(select_items)
    for index, item in enumerate(select_items):
        alias = item.get("alias")
        expression_sql = item.get("expression")
        if not expression_sql:
            continue
        if use_view_names:
            column_name = view_names[index]
        else:
            column_name = alias or expression_sql
        columns.append({"name": column_name, "source_column": expression_sql})
    return columns


def _get_source_table_name(query_block: dict[str, Any]) -> Optional[str]:
    """Возвращает имя первой таблицы из FROM."""
    sources = query_block.get("from") or []
    table = _find_first_table_in_sources(sources)
    if table:
        return table.get("name")
    for join in query_block.get("joins", []):
        table = _find_first_table_in_sources([join.get("source")])
        if table:
            return table.get("name")
    return None


def _collect_dependencies(query_blocks: list[dict[str, Any]]) -> list[dict[str, str]]:
    """Собирает список таблиц-источников по всем блокам."""
    seen: set[tuple[Optional[str], Optional[str]]] = set()
    dependencies: list[dict[str, str]] = []
    for block in query_blocks:
        for source in block.get("from", []):
            _collect_dependencies_from_source(dependencies, seen, source)
        for join in block.get("joins", []):
            _collect_dependencies_from_source(dependencies, seen, join.get("source"))
    return dependencies


def _append_dependency(
    dependencies: list[dict[str, str]],
    seen: set[tuple[Optional[str], Optional[str]]],
    source: Optional[dict[str, Any]],
) -> None:
    """Добавляет зависимость-таблицу в общий список без дублей."""
    if not source or source.get("type") != "table":
        return
    name = source.get("name")
    schema = source.get("schema")
    key = (schema, name)
    if key in seen:
        return
    seen.add(key)
    if not name:
        return
    entry: dict[str, str] = {"type": "table", "name": name}
    if schema:
        entry["schema"] = schema
    dependencies.append(entry)


def _collect_dependencies_from_source(
    dependencies: list[dict[str, str]],
    seen: set[tuple[Optional[str], Optional[str]]],
    source: Optional[dict[str, Any]],
) -> None:
    """Добавляет зависимости из источника, включая подзапросы."""
    if not source:
        return
    if source.get("type") == "table":
        _append_dependency(dependencies, seen, source)
        return
    if source.get("type") == "subquery":
        query = source.get("query")
        if query:
            for sub_source in query.get("from", []):
                _collect_dependencies_from_source(dependencies, seen, sub_source)
            for join in query.get("joins", []):
                _collect_dependencies_from_source(dependencies, seen, join.get("source"))


def _find_first_table_in_sources(sources: list[Optional[dict[str, Any]]]) -> Optional[dict[str, Any]]:
    """Ищет первую таблицу в списке источников, включая подзапросы."""
    for source in sources:
        if not source:
            continue
        if source.get("type") == "table":
            return source
        if source.get("type") == "subquery":
            query = source.get("query")
            if query:
                table = _find_first_table_in_sources(query.get("from", []))
                if table:
                    return table
    return None


def _collect_query_level(expression: exp.Expression, dialect: Optional[str]) -> Optional[dict[str, Any]]:
    """Собирает параметры верхнего уровня Query (ORDER/LIMIT/OFFSET/FETCH)."""
    if not isinstance(expression, exp.Query):
        return None
    order_by = _collect_order_by(expression, dialect)
    limit = _collect_limit(expression, dialect)
    offset = _collect_offset(expression, dialect)
    fetch = _collect_fetch(expression, dialect)
    if not any([order_by, limit, offset, fetch]):
        return None
    return {
        "order_by": order_by,
        "limit": limit,
        "offset": offset,
        "fetch": fetch,
    }


def _normalize_view_column_names(columns: list[str]) -> list[str]:
    """Нормализует имена колонок из определения VIEW."""
    result: list[str] = []
    for column in columns:
        name = _normalize_view_column_name(column)
        if name:
            result.append(name)
    return result


def _normalize_view_column_name(column: str) -> Optional[str]:
    """Возвращает имя колонки из определения вида 'col TYPE'."""
    if not column:
        return None
    match = re.match(r'^\s*("([^"]+)"|`([^`]+)`|\[([^\]]+)\]|([^\s,]+))', column)
    if match:
        return next(value for value in match.groups()[1:] if value)
    return column.strip().split()[0]


def _empty_view_json(view_name: str) -> dict[str, Any]:
    """Возвращает пустой JSON для VIEW, если DDL не разобран."""
    return {
        "type": "VIEW",
        "name": view_name,
        "view_columns": None,
        "queries": [],
        "source_table": None,
        "columns": [],
        "where_condition": None,
        "dependencies": [],
    }
