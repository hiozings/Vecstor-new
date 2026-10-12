"""Validate SQL ASTs and emit a parameterized metadata-only SELECT."""

from dataclasses import dataclass
from decimal import Decimal, DecimalException
import math

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError


METADATA = ("id", "category", "timestamp", "value")
COMPARISONS = (exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE)


class QueryError(ValueError):
    """SQL is malformed or outside the supported subset."""


@dataclass(frozen=True)
class QueryPlan:
    projection: tuple[str, ...]
    limit: int
    filter_sql: str
    filter_params: tuple


def _only(node, allowed):
    if any(key not in allowed and value is not None and value is not False and value != []
           for key, value in node.args.items()):
        raise QueryError(f"Unsupported options on {type(node).__name__}")


def _identifier(node):
    if not isinstance(node, exp.Identifier):
        raise QueryError("Expected a simple identifier")
    _only(node, {"this", "quoted"})
    return node.name.lower()


def _column(node, allowed=METADATA):
    if not isinstance(node, exp.Column):
        raise QueryError("Expected a plain column without alias or expression")
    _only(node, {"this"})
    name = _identifier(node.this)
    if name not in allowed:
        raise QueryError(f"Unsupported column: {name}")
    return name


def _number(node, integer=False):
    sign = 1
    if isinstance(node, exp.Neg):
        _only(node, {"this"})
        sign, node = -1, node.this
    if not isinstance(node, exp.Literal) or node.is_string:
        raise QueryError("Expected a numeric literal")
    _only(node, {"this", "is_string"})
    if integer and not (node.this.isascii() and node.this.isdecimal()):
        raise QueryError("Expected an integer literal")
    try:
        value = Decimal(node.this)
        if sign < 0:
            value = value.copy_negate()
        if not value.is_finite():
            raise QueryError("Numeric literal must be finite")
        if integer:
            if not -(2**63) <= value < 2**63:
                raise QueryError("Integer literal is outside int64 range")
            return int(value)
        result = float(value)
        if not math.isfinite(result):
            raise QueryError("Numeric literal overflows float64")
        return result
    except (DecimalException, OverflowError) as error:
        raise QueryError("Invalid numeric literal") from error


def _predicate(node, params, depth=0):
    if depth > 64:
        raise QueryError("Predicate nesting exceeds 64 levels")
    if isinstance(node, exp.Paren):
        _only(node, {"this"})
        return exp.Paren(this=_predicate(node.this, params, depth + 1))
    if type(node) in (exp.And, exp.Or):
        _only(node, {"this", "expression"})
        return type(node)(this=_predicate(node.this, params, depth + 1),
                          expression=_predicate(node.expression, params, depth + 1))
    if type(node) not in COMPARISONS:
        raise QueryError("WHERE supports comparisons joined by AND/OR only")
    _only(node, {"this", "expression"})
    column = _column(node.this)
    literal = node.expression
    if column == "category":
        if type(node) not in (exp.EQ, exp.NEQ) or not isinstance(literal, exp.Literal) or not literal.is_string:
            raise QueryError("category requires equality/inequality with a string literal")
        _only(literal, {"this", "is_string"})
        value = literal.this
    else:
        value = _number(literal, integer=column in {"id", "timestamp"})
    params.append(value)
    return type(node)(this=exp.column(column, quoted=True), expression=exp.Placeholder())


def parse_query(sql: str) -> QueryPlan:
    if not isinstance(sql, str) or not sql.strip() or len(sql) > 8192:
        raise QueryError("SQL must be nonempty and at most 8192 characters")
    try:
        statements = sqlglot.parse(sql, read="duckdb")
    except (SqlglotError, RecursionError) as error:
        raise QueryError(f"Cannot parse SQL: {error}") from error
    if len(statements) != 1 or not isinstance(statements[0], exp.Select):
        raise QueryError("Exactly one SELECT is required")
    tree = statements[0]
    if sum(1 for _ in tree.walk()) > 256:
        raise QueryError("SQL AST exceeds 256 nodes")
    _only(tree, {"expressions", "from_", "where", "order", "limit"})
    from_node = tree.args.get("from_")
    if not isinstance(from_node, exp.From) or not isinstance(from_node.this, exp.Table):
        raise QueryError("FROM items is required")
    _only(from_node, {"this"})
    _only(from_node.this, {"this"})
    if _identifier(from_node.this.this) != "items":
        raise QueryError("Only the items table is supported")

    projected = tree.expressions
    if len(projected) == 1 and isinstance(projected[0], exp.Star):
        _only(projected[0], set())
        projection = METADATA
    else:
        projection = tuple(_column(node) for node in projected)
    if not projection or len(set(projection)) != len(projection):
        raise QueryError("Projection must contain distinct metadata columns")

    limit_node = tree.args.get("limit")
    if not isinstance(limit_node, exp.Limit):
        raise QueryError("A positive integer LIMIT is required")
    _only(limit_node, {"expression"})
    limit = _number(limit_node.expression, integer=True)
    if limit < 1:
        raise QueryError("LIMIT must be positive")

    order = tree.args.get("order")
    if not isinstance(order, exp.Order) or len(order.expressions) not in (1, 2):
        raise QueryError("ORDER BY vector_distance(vector, :query_vector) is required")
    _only(order, {"expressions"})
    for item in order.expressions:
        _only(item, {"this", "desc", "nulls_first"})
        if item.args.get("desc"):
            raise QueryError("Only ascending order is supported")
    distance = order.expressions[0].this
    if not isinstance(distance, exp.Anonymous) or distance.name.lower() != "vector_distance":
        raise QueryError("Expected vector_distance(vector, :query_vector)")
    _only(distance, {"this", "expressions"})
    if len(distance.expressions) != 2:
        raise QueryError("vector_distance expects two arguments")
    _column(distance.expressions[0], allowed=("vector",))
    parameter = distance.expressions[1]
    if not isinstance(parameter, exp.Placeholder) or parameter.this != "query_vector":
        raise QueryError("Query vector must be supplied as :query_vector")
    _only(parameter, {"this"})
    if len(order.expressions) == 2 and _column(order.expressions[1].this) != "id":
        raise QueryError("Secondary order may only be id ASC")

    params = []
    statement = exp.select(*(exp.column(name, quoted=True) for name in ("_row", *METADATA))).from_("items")
    where = tree.args.get("where")
    if where is not None:
        _only(where, {"this"})
        statement = statement.where(_predicate(where.this, params))
    return QueryPlan(projection, limit, statement.sql(dialect="duckdb"), tuple(params))
