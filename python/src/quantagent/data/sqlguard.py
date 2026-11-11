"""Allow-list validation of a parsed SQL statement.

``PITSession.sql`` asks DuckDB to parse the query into its own syntax tree
(``json_serialize_sql``), passes the tree here, and then executes that same
tree. The text of the query is never searched for forbidden words, and there
is no second parser that could disagree with the engine about what the text
means.

The rule is allow-listing: a statement is accepted only if every node in its
tree is of a known kind, every table is one of the session's views, and every
function is on the list below. Anything this module has not been taught is
refused, including syntax added by a future DuckDB version.
"""

from collections.abc import Collection
from typing import Any

MAX_QUERY_LENGTH = 100_000


class QueryRejected(ValueError):
    """The statement is not a single allowed SELECT. It was not executed."""


# --- allow-lists -----------------------------------------------------------------

_QUERY_NODES = {
    "SELECT_NODE": {
        "type", "modifiers", "cte_map", "select_list", "from_table", "where_clause",
        "group_expressions", "group_sets", "aggregate_handling", "having", "sample", "qualify",
    },
    "SET_OPERATION_NODE": {
        "type", "modifiers", "cte_map", "setop_type", "left", "right", "setop_all", "children",
    },
}  # fmt: skip

_TABLE_REFS = {
    "BASE_TABLE": {
        "type", "alias", "sample", "query_location", "query_location_length", "schema_name",
        "table_name", "column_name_alias", "catalog_name", "at_clause", "qualified_name",
    },
    "JOIN": {
        "type", "alias", "sample", "query_location", "query_location_length", "left", "right",
        "condition", "join_type", "ref_type", "using_columns", "delim_flipped",
        "duplicate_eliminated_columns", "is_implicit", "nearest_approx", "nearest_count",
        "nearest_order_type", "ranking_expression",
    },
    "SUBQUERY": {
        "type", "alias", "sample", "query_location", "query_location_length", "subquery",
        "column_name_alias",
    },
    "EMPTY": {"type", "alias", "sample", "query_location", "query_location_length"},
}  # fmt: skip

# Positional joins depend on physical row order, so they are left out.
_JOIN_KINDS = {"REGULAR", "NATURAL", "CROSS", "ASOF", "DEPENDENT"}

_MODIFIERS = {"ORDER_MODIFIER", "LIMIT_MODIFIER", "DISTINCT_MODIFIER"}
_ORDERINGS = {"ORDER_DEFAULT", "ASCENDING", "DESCENDING"}

_EXPRESSION_CLASSES = {
    "BETWEEN", "CASE", "CAST", "COLUMN_REF", "COMPARISON", "CONJUNCTION", "CONSTANT",
    "FUNCTION", "OPERATOR", "STAR", "SUBQUERY", "TYPE", "WINDOW",
}  # fmt: skip

_OPERATORS = {
    "COMPARE_IN", "COMPARE_NOT_IN", "OPERATOR_NOT", "OPERATOR_IS_NULL", "OPERATOR_IS_NOT_NULL",
    "OPERATOR_COALESCE",
}  # fmt: skip

_FUNCTION_KEYS = {
    "class", "type", "alias", "query_location", "query_location_length", "function_name",
    "schema", "catalog", "arguments", "distinct", "filter", "order_bys", "export_state",
    "is_operator", "qualified_name",
}  # fmt: skip

_WINDOW_KEYS = {
    "class", "type", "alias", "query_location", "query_location_length", "function_name",
    "schema", "catalog", "arguments", "arg_orders", "partitions", "orders", "start", "end",
    "start_expr", "end_expr", "offset_expr", "default_expr", "ignore_nulls", "has_ignore_nulls",
    "filter_expr", "exclude_clause", "distinct", "qualified_name",
}  # fmt: skip

# Pure, deterministic functions only. Nothing that reads settings, files, the
# clock, the catalog or random state, and nothing that runs SQL from a string.
ALLOWED_FUNCTIONS = frozenset({
    # arithmetic and string operators, as DuckDB names them in the tree
    "+", "-", "*", "/", "//", "%", "**", "^", "||", "~~", "!~~", "~~*", "!~~*",
    # aggregates
    "count", "count_star", "count_if", "sum", "product", "min", "max", "avg", "mean",
    "first", "last", "any_value", "arg_min", "arg_max", "min_by", "max_by",
    "stddev", "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop",
    "median", "quantile", "quantile_cont", "quantile_disc", "corr", "covar_samp", "covar_pop",
    "bool_and", "bool_or",
    # window
    "row_number", "rank", "dense_rank", "percent_rank", "cume_dist", "ntile",
    "lag", "lead", "first_value", "last_value", "nth_value",
    # scalar numeric
    "abs", "sign", "round", "floor", "ceil", "ceiling", "trunc", "sqrt", "ln", "log", "log2",
    "log10", "exp", "pow", "power", "greatest", "least",
    # null handling
    "coalesce", "nullif", "ifnull",
    # strings
    "lower", "upper", "length", "concat", "substring", "substr", "starts_with", "contains",
})  # fmt: skip

ALLOWED_TYPES = frozenset({
    "TINYINT", "INT1", "SMALLINT", "INT2", "INTEGER", "INT", "INT4", "BIGINT", "INT8", "LONG",
    "HUGEINT", "UINTEGER", "UBIGINT", "DOUBLE", "FLOAT8", "FLOAT", "FLOAT4", "REAL", "DECIMAL",
    "NUMERIC", "VARCHAR", "TEXT", "STRING", "BOOLEAN", "BOOL", "DATE", "TIMESTAMP",
    "TIMESTAMP_NS",
})  # fmt: skip

# SQL lets these be written as bare identifiers. They parse as column
# references but bind to functions (the clock, the user, the catalog) when no
# column has that name, so they cannot be told apart from columns in the tree.
# The set is fixed by the SQL grammar. They are refused as unqualified column
# names and as aliases.
VALUE_FUNCTION_NAMES = frozenset({
    "current_date", "current_time", "current_timestamp", "localtime", "localtimestamp",
    "current_role", "current_user", "session_user", "user", "current_catalog", "current_schema",
})  # fmt: skip


def _shown(name: Any) -> str:
    """A name for an error message, unless it is something like a file path."""
    text = str(name)
    if len(text) <= 64 and text.replace("_", "").isalnum() and text.isascii():
        return repr(text)
    return "<name>"


def _check_keys(node: dict, allowed: set[str], what: str) -> None:
    unknown = set(node) - allowed
    if unknown:
        raise QueryRejected(f"{what} has unsupported properties: {sorted(unknown)}")


def _check_unqualified(node: dict, name: str, what: str) -> None:
    path = (node.get("qualified_name") or {}).get("path", [name])
    if len(path) != 1 or path[0] != name:
        raise QueryRejected(f"qualified {what} names are not allowed")


def _check_alias(name: Any) -> None:
    if isinstance(name, str) and name.lower() in VALUE_FUNCTION_NAMES:
        raise QueryRejected(f"{_shown(name)} cannot be used as a name")


def _check_table_ref(node: dict, tables: Collection[str]) -> None:
    kind = node["type"]
    _check_keys(node, _TABLE_REFS[kind], f"{kind} reference")
    if kind == "BASE_TABLE":
        if node.get("catalog_name") or node.get("schema_name"):
            raise QueryRejected("qualified table names are not allowed")
        name = node.get("table_name")
        _check_unqualified(node, name, "table")
        if not isinstance(name, str) or name.lower() not in tables:
            raise QueryRejected(
                f"table {_shown(name)} is not allowed; the tables are {sorted(tables)}"
            )
    elif kind == "JOIN":
        if node.get("ref_type") not in _JOIN_KINDS:
            raise QueryRejected(f"join kind {_shown(node.get('ref_type'))} is not allowed")


def _check_node(node: dict, tables: Collection[str]) -> None:
    kind = node["type"]
    if kind in _QUERY_NODES:
        _check_keys(node, _QUERY_NODES[kind], kind)
    elif kind in _TABLE_REFS:
        _check_table_ref(node, tables)
    elif kind == "LIMIT_MODIFIER":
        if node.get("limit_type") != "ROW_COUNT":
            raise QueryRejected("only a row-count LIMIT is allowed")
    elif kind in _MODIFIERS or kind in _ORDERINGS:
        pass
    elif kind == "TABLE_FUNCTION":
        function = node.get("function") or {}
        raise QueryRejected(
            f"table function {_shown(function.get('function_name'))} is not allowed; "
            "query the tables directly"
        )
    elif kind == "SHOW_REF":
        raise QueryRejected("SHOW, DESCRIBE and SUMMARIZE are not allowed")
    else:
        raise QueryRejected(f"{_shown(kind)} is not allowed")


def _check_expression(node: dict) -> None:
    kind = node["class"]
    if kind not in _EXPRESSION_CLASSES:
        raise QueryRejected(f"expression type {kind} is not allowed")

    if kind in ("FUNCTION", "WINDOW"):
        _check_keys(node, _FUNCTION_KEYS if kind == "FUNCTION" else _WINDOW_KEYS, "function call")
        name = node.get("function_name")
        if node.get("catalog") or node.get("schema"):
            raise QueryRejected("qualified function names are not allowed")
        _check_unqualified(node, name, "function")
        if not isinstance(name, str) or name.lower() not in ALLOWED_FUNCTIONS:
            raise QueryRejected(f"function {_shown(name)} is not allowed")
        if node.get("export_state"):
            raise QueryRejected("EXPORT_STATE is not allowed")
    elif kind == "COLUMN_REF":
        names = node.get("column_names")
        if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
            raise QueryRejected("malformed column reference")
        if len(names) == 1 and names[0].lower() in VALUE_FUNCTION_NAMES:
            raise QueryRejected(f"{_shown(names[0])} is not allowed")
    elif kind == "TYPE":
        name = node.get("type_name")
        if node.get("catalog") or node.get("schema"):
            raise QueryRejected("qualified type names are not allowed")
        _check_unqualified(node, name, "type")
        if not isinstance(name, str) or name.upper() not in ALLOWED_TYPES:
            raise QueryRejected(f"type {_shown(name)} is not allowed")
    elif kind == "OPERATOR":
        if node.get("type") not in _OPERATORS:
            raise QueryRejected(f"operator {_shown(node.get('type'))} is not allowed")


def _walk(node: Any, tables: Collection[str]) -> None:
    if isinstance(node, list):
        for item in node:
            _walk(item, tables)
        return
    if not isinstance(node, dict):
        return

    if "class" in node:
        _check_expression(node)
    elif isinstance(node.get("type"), str):
        _check_node(node, tables)

    # Rules that hold wherever these properties appear.
    if node.get("sample") is not None:
        raise QueryRejected("sampling is not allowed")
    if node.get("at_clause") is not None:
        raise QueryRejected("AT (time travel) is not allowed")
    if "cte_map" in node and node["cte_map"] != {"map": []}:
        # A WITH name can stand in front of a catalog object of the same name
        # in one place and not in another, which would defeat the table
        # allow-list. Subqueries in FROM do the same job.
        raise QueryRejected("WITH (common table expressions) is not allowed; use a subquery")
    _check_alias(node.get("alias"))
    for alias in node.get("column_name_alias") or []:
        _check_alias(alias)

    if node.get("class") == "CONSTANT":
        return  # a literal value; nothing inside it is executable
    for value in node.values():
        _walk(value, tables)


def validate(tree: Any, tables: Collection[str]) -> None:
    """Raise QueryRejected unless ``tree`` is exactly one allowed SELECT.

    ``tree`` is the decoded output of DuckDB's ``json_serialize_sql``.
    ``tables`` are the lower-case names a query may read from.
    """
    if not isinstance(tree, dict):
        raise QueryRejected("could not parse the statement")
    if tree.get("error"):
        if tree.get("error_type") == "parser":
            raise QueryRejected(f"not valid SQL: {tree.get('error_message')}")
        raise QueryRejected("only a single SELECT statement is allowed")

    statements = tree.get("statements")
    if not isinstance(statements, list) or len(statements) != 1:
        count = len(statements) if isinstance(statements, list) else 0
        raise QueryRejected(f"expected exactly one statement, found {count}")
    statement = statements[0]
    if set(statement) != {"node", "named_param_map"} or statement["named_param_map"]:
        raise QueryRejected("expression type PARAMETER is not allowed")
    node = statement["node"]
    if not isinstance(node, dict) or node.get("type") not in _QUERY_NODES:
        raise QueryRejected("only a single SELECT statement is allowed")

    try:
        _walk(node, tables)
    except RecursionError:
        raise QueryRejected("the statement is nested too deeply") from None
