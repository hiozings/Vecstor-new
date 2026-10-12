"""Bounded, deny-by-default SQL grammar for the managed P2.5 SQL entry point.

Only SQL emitted from the validated AST may be passed to DuckDB. Database
commands are routing instructions, and vector plans are executed by compute.
"""
from dataclasses import dataclass
import re

import numpy as np
import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError
from sqlglot.tokens import TokenType

from .parser import METADATA, QueryError, _only, _number, _predicate


@dataclass(frozen=True)
class VectorPlan:
    table: str
    metadata_sql: str
    query_vector: tuple[float, ...]
    limit: int
    projection: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class StatementPlan:
    kind: str
    sql: str
    table: str | None = None
    database_name: str | None = None
    vector: VectorPlan | None = None

    @property
    def is_write(self) -> bool:
        return self.kind in {'create', 'drop', 'insert', 'update', 'delete'}


_RESERVED = {'main', 'temp', 'information_schema', 'pg_catalog', 'sqlite_master', 'sqlite_schema'}


def safe_name(value: str) -> str:
    """Validate a logical database/table/column name, never a filesystem path."""
    if not isinstance(value, str) or not re.fullmatch(r'[A-Za-z][A-Za-z0-9_]*', value):
        raise QueryError('Names must start with an ASCII letter and contain letters, digits or underscores')
    name = value.lower()
    if name in _RESERVED or name.startswith(('duckdb_', 'sqlite_', 'pg_', 'vecstor_')):
        raise QueryError('Reserved or internal name')
    return name


def _identifier(node):
    if type(node) is not exp.Identifier:
        raise QueryError('Expected a simple identifier')
    _only(node, {'this', 'quoted'})
    name = safe_name(node.name)
    node.set('this', name)
    node.set('quoted', True)
    return name


def _table(node):
    if type(node) is not exp.Table:
        raise QueryError('Expected a single named table')
    _only(node, {'this'})
    return _identifier(node.this)


def _column(node):
    if type(node) is not exp.Column:
        raise QueryError('Expected an unqualified column')
    _only(node, {'this'})
    return _identifier(node.this)


def _array(node):
    if type(node) is not exp.Array:
        raise QueryError('Expected an inline numeric vector')
    _only(node, {'expressions'})
    if not node.expressions:
        raise QueryError('Vector must not be empty')
    values = [_number(item) for item in node.expressions]
    with np.errstate(over='ignore', invalid='ignore'):
        vector = np.asarray(values, dtype=np.float32)
    if not np.all(np.isfinite(vector)):
        raise QueryError('Vector must be finite float32')
    return tuple(float(x) for x in vector)


_BINARY = {exp.Add, exp.Sub, exp.Mul, exp.Div, exp.Mod, exp.EQ, exp.NEQ, exp.LT, exp.LTE, exp.GT, exp.GTE, exp.And, exp.Or}
_AGGREGATES = {exp.Count, exp.Sum, exp.Avg, exp.Min, exp.Max}


def _expression(node, *, aggregate=False, star=False, literal_only=False, depth=0):
    if depth > 64:
        raise QueryError('Expression nesting exceeds 64 levels')
    typ = type(node)
    if typ is exp.Literal:
        _only(node, {'this', 'is_string'})
        if not node.is_string:
            _number(node)
        return
    if typ in {exp.Null, exp.Boolean}:
        _only(node, {'this'} if typ is exp.Boolean else set())
        return
    if typ is exp.Array:
        _array(node)
        return
    if typ is exp.Neg:
        _only(node, {'this'})
        if literal_only:
            _number(node)
        else:
            _expression(node.this, depth=depth+1, aggregate=aggregate)
        return
    if literal_only:
        raise QueryError('INSERT VALUES accepts only literals')
    if typ is exp.Column:
        _column(node)
    elif typ is exp.Star and star:
        _only(node, set())
    elif typ is exp.Paren:
        _only(node, {'this'})
        _expression(node.this, aggregate=aggregate, depth=depth+1)
    elif typ in _BINARY:
        _only(node, {'this', 'expression', 'typed', 'safe'} if typ is exp.Div else {'this', 'expression'})
        for child in (node.this, node.expression):
            _expression(child, aggregate=aggregate, depth=depth+1)
    elif typ is exp.Is:
        _only(node, {'this', 'expression'})
        if type(node.expression) is not exp.Null:
            raise QueryError('IS supports NULL only')
        _expression(node.this, aggregate=aggregate, depth=depth+1)
    elif typ is exp.Not:
        _only(node, {'this'})
        if type(node.this) is not exp.Is:
            raise QueryError('NOT supports IS NOT NULL only')
        _expression(node.this, aggregate=aggregate, depth=depth+1)
    elif typ in _AGGREGATES and aggregate:
        _only(node, {'this', 'big_int'} if typ is exp.Count else {'this'})
        _expression(node.this, star=typ is exp.Count, depth=depth+1)
    else:
        raise QueryError(f'Unsupported expression: {typ.__name__}')


def _where(tree):
    where = tree.args.get('where')
    if where is not None:
        _only(where, {'this'})
        _expression(where.this)


def _type(node):
    if type(node) is not exp.DataType:
        raise QueryError('Expected a supported column type')
    if node.this == exp.DataType.Type.ARRAY:
        _only(node, {'this', 'expressions', 'values', 'nested'})
        if len(node.expressions) != 1 or node.expressions[0].this != exp.DataType.Type.FLOAT:
            raise QueryError('Only FLOAT[D] arrays are supported')
        _only(node.expressions[0], {'this', 'nested'})
        sizes = node.args.get('values') or []
        if len(sizes) != 1 or not 1 <= _number(sizes[0], integer=True) <= 65536:
            raise QueryError('Array dimension must be between 1 and 65536')
    else:
        _only(node, {'this', 'nested'})
        if node.this not in {exp.DataType.Type.BOOLEAN, exp.DataType.Type.INT, exp.DataType.Type.BIGINT, exp.DataType.Type.DOUBLE, exp.DataType.Type.TEXT, exp.DataType.Type.VARCHAR}:
            raise QueryError('Unsupported column type')


def _distance(node):
    if type(node) is not exp.Anonymous or node.name.lower() != 'vector_distance':
        raise QueryError('Expected vector_distance(vector, [literal components])')
    _only(node, {'this', 'expressions'})
    if len(node.expressions) != 2 or _column(node.expressions[0]) != 'vector':
        raise QueryError('vector_distance requires vector and a numeric array')
    return _array(node.expressions[1])


def _vector(tree, table):
    _only(tree, {'expressions', 'from_', 'where', 'order', 'limit'})
    order = tree.args.get('order')
    if type(order) is not exp.Order or len(order.expressions) not in (1,2):
        raise QueryError('Vector query requires ORDER BY distance and optional id')
    _only(order, {'expressions'})
    for item in order.expressions:
        _only(item, {'this', 'desc', 'nulls_first'})
        if item.args.get('desc'):
            raise QueryError('Vector ordering must be ascending')
    if len(order.expressions) == 2 and _column(order.expressions[1].this) != 'id':
        raise QueryError('Vector secondary ordering must be id')
    projection = []
    distances = []
    aliases = {}
    for item in tree.expressions:
        alias = None
        if type(item) is exp.Alias:
            _only(item, {'this', 'alias'})
            alias = _identifier(item.args['alias'])
            item = item.this
        if type(item) is exp.Star and alias is None:
            _only(item, set())
            projection.extend((name,name) for name in (*METADATA, 'vector'))
        elif type(item) is exp.Column:
            name = _column(item)
            if name not in (*METADATA, 'vector'):
                raise QueryError('Unknown vector projection column')
            projection.append((alias or name, name))
        else:
            value = _distance(item)
            distances.append(value)
            output = alias or 'distance'
            projection.append((output, 'distance'))
            aliases[output] = value
    if not projection or len({name for name,_ in projection}) != len(projection):
        raise QueryError('Vector projection requires unique output names')
    first = order.expressions[0].this
    if type(first) is exp.Column:
        vector = aliases.get(_column(first))
        if vector is None:
            raise QueryError('ORDER BY alias must refer to projected vector_distance')
    else:
        vector = _distance(first)
    if any(value != vector for value in distances):
        raise QueryError('All vector_distance expressions must use the same vector')
    limit = tree.args.get('limit')
    if type(limit) is not exp.Limit:
        raise QueryError('Vector query requires a positive LIMIT')
    _only(limit, {'expression'})
    count = _number(limit.expression, integer=True)
    if count < 1:
        raise QueryError('Vector LIMIT must be positive')
    metadata = exp.select(*(exp.column(name, quoted=True) for name in METADATA)).from_(exp.Table(this=exp.to_identifier(table, quoted=True)))
    where = tree.args.get('where')
    if where is not None:
        _only(where, {'this'})
        # Reuse the exact P1 metadata predicate semantics; rebuild literals from
        # validated bound values rather than copying arbitrary input AST nodes.
        params = []
        predicate = _predicate(where.this, params)
        iterator = iter(params)
        predicate = predicate.transform(lambda node: exp.convert(next(iterator)) if type(node) is exp.Placeholder else node)
        metadata = metadata.where(predicate)
    return VectorPlan(table, metadata.sql(dialect='duckdb'), vector, count, tuple(projection))


def _classify(tree):
    if type(tree) is exp.Command:
        _only(tree, {'this', 'expression'})
        # SQLGlot represents SHOW DATABASES as Command; lex the narrow tail
        # to discard comments without admitting arbitrary command SQL.
        tail = sqlglot.Dialect.get_or_raise('duckdb').tokenize(str(tree.args.get('expression', '')))
        if tree.this.upper() == 'SHOW' and len(tail) == 1 and tail[0].token_type is TokenType.VAR and tail[0].text.upper() == 'DATABASES':
            return StatementPlan('show_databases', 'SHOW DATABASES')
        raise QueryError('Unsupported SQL command')
    if type(tree) is exp.Show:
        _only(tree, {'this'})
        if tree.this != 'TABLES':
            raise QueryError('Only SHOW TABLES is supported')
        return StatementPlan('show_tables', 'SHOW TABLES')
    if type(tree) is exp.Use:
        _only(tree, {'this'})
        name = _table(tree.this)
        return StatementPlan('use', tree.sql(dialect='duckdb'), database_name=name)
    if type(tree) is exp.Create and tree.args.get('kind') == 'DATABASE':
        _only(tree, {'this', 'kind'})
        name = _table(tree.this)
        return StatementPlan('create_database', tree.sql(dialect='duckdb'), database_name=name)
    if type(tree) is exp.Describe:
        _only(tree, {'this'})
        table = _table(tree.this)
        return StatementPlan('describe', tree.sql(dialect='duckdb'), table=table)
    if type(tree) is exp.Create:
        _only(tree, {'this', 'kind'})
        if tree.args.get('kind') != 'TABLE' or type(tree.this) is not exp.Schema:
            raise QueryError('CREATE TABLE requires explicit column definitions')
        schema = tree.this
        _only(schema, {'this', 'expressions'})
        table = _table(schema.this)
        names = []
        for column in schema.expressions:
            if type(column) is not exp.ColumnDef:
                raise QueryError('Only column definitions are supported')
            _only(column, {'this','kind','constraints'})
            names.append(_identifier(column.this))
            _type(column.args['kind'])
            for constraint in column.args.get('constraints') or []:
                _only(constraint, {'kind'})
                kind = constraint.args.get('kind')
                if type(kind) not in {exp.PrimaryKeyColumnConstraint, exp.NotNullColumnConstraint}:
                    raise QueryError('Only PRIMARY KEY and NOT NULL constraints are supported')
                _only(kind, set())
        if not names or len(set(names)) != len(names):
            raise QueryError('Column names must be nonempty and unique')
        kind = 'create'
    elif type(tree) is exp.Drop:
        _only(tree, {'tables','kind'})
        tables = tree.args.get('tables') or []
        if tree.args.get('kind') != 'TABLE' or len(tables) != 1:
            raise QueryError('Only DROP TABLE of one table is supported')
        table, kind = _table(tables[0]), 'drop'
    elif type(tree) is exp.Insert:
        _only(tree, {'this','expression'})
        target = tree.this
        if type(target) is exp.Schema:
            _only(target, {'this','expressions'})
            names = [_identifier(item) for item in target.expressions]
            if not names or len(names) != len(set(names)):
                raise QueryError('INSERT columns must be distinct')
            target = target.this
        table = _table(target)
        values = tree.expression
        if type(values) is not exp.Values or not values.expressions:
            raise QueryError('INSERT requires VALUES rows')
        _only(values, {'expressions'})
        width = None
        for row in values.expressions:
            if type(row) is not exp.Tuple or not row.expressions:
                raise QueryError('Expected nonempty VALUES row')
            _only(row, {'expressions'})
            if width is not None and width != len(row.expressions):
                raise QueryError('VALUES rows must have equal width')
            width = len(row.expressions)
            for item in row.expressions:
                _expression(item, literal_only=True)
        kind = 'insert'
    elif type(tree) is exp.Update:
        _only(tree, {'this','expressions','where'})
        table, kind = _table(tree.this), 'update'
        assigned = []
        for assignment in tree.expressions:
            if type(assignment) is not exp.EQ:
                raise QueryError('Expected column assignment')
            _only(assignment, {'this','expression'})
            assigned.append(_column(assignment.this))
            _expression(assignment.expression)
        if not assigned or len(assigned) != len(set(assigned)):
            raise QueryError('Assignments must target distinct columns')
        _where(tree)
    elif type(tree) is exp.Delete:
        _only(tree, {'this','where'})
        table, kind = _table(tree.this), 'delete'
        _where(tree)
    elif type(tree) is exp.Select:
        _only(tree, {'expressions','from_','where','group','having','order','limit','offset'})
        source = tree.args.get('from_')
        if type(source) is not exp.From:
            raise QueryError('SELECT requires one table')
        _only(source, {'this'})
        table = _table(source.this)
        if any(type(node) is exp.Anonymous and node.name.lower() == 'vector_distance' for node in tree.walk()):
            vector = _vector(tree, table)
            return StatementPlan('vector', tree.sql(dialect='duckdb'), table=table, vector=vector)
        if not tree.expressions:
            raise QueryError('SELECT requires projections')
        for item in tree.expressions:
            if type(item) is exp.Alias:
                _only(item, {'this','alias'})
                _identifier(item.args['alias'])
                item = item.this
            _expression(item, aggregate=True, star=True)
        _where(tree)
        group = tree.args.get('group')
        if group is not None:
            _only(group, {'expressions'})
            for item in group.expressions:
                _expression(item)
        having = tree.args.get('having')
        if having is not None:
            _only(having, {'this'})
            _expression(having.this, aggregate=True)
        order = tree.args.get('order')
        if order is not None:
            _only(order, {'expressions'})
            for item in order.expressions:
                _only(item, {'this','desc','nulls_first'})
                _expression(item.this, aggregate=True)
        for key in ('limit','offset'):
            node = tree.args.get(key)
            if node is not None:
                _only(node, {'expression'})
                if _number(node.expression, integer=True) < 0:
                    raise QueryError('LIMIT/OFFSET must be nonnegative')
        kind = 'select'
    else:
        raise QueryError(f'Unsupported statement: {type(tree).__name__}')
    return StatementPlan(kind, tree.sql(dialect='duckdb'), table=table)


def classify_statement(sql: str) -> StatementPlan:
    if not isinstance(sql, str) or not sql.strip() or len(sql) > 65536:
        raise QueryError('SQL must be nonempty and at most 65536 characters')
    try:
        statements = sqlglot.parse(sql, read='duckdb')
        if len(statements) != 1 or statements[0] is None:
            raise QueryError('Exactly one statement is required')
        tree = statements[0]
        if sum(1 for _ in tree.walk()) > 4096:
            raise QueryError('SQL AST exceeds 4096 nodes')
        # Comments carry no SQL semantics and are not emitted to the executor.
        for node in tree.walk():
            node.pop_comments()
        return _classify(tree)
    except (SqlglotError, RecursionError, ValueError, OverflowError) as error:
        if isinstance(error, QueryError):
            raise
        raise QueryError(f'Invalid SQL: {error}') from error
