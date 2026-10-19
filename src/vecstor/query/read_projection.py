"""Canonical metadata projections derived from validated vector-query plans."""
import sqlglot
from sqlglot import exp

FIELDS=('id','category','timestamp','value')


def validate_fields(fields):
    fields=tuple(fields)
    if not fields or fields[0]!='id' or fields!=tuple(name for name in FIELDS if name in fields):
        raise ValueError('Invalid projected metadata fields')
    return fields


def required_metadata_fields(plan, projected):
    wanted={source for _,source in plan.projection}|{'id'}
    return tuple(name for name in FIELDS if not projected or name in wanted)


def project_metadata_sql(metadata_sql, fields):
    fields=validate_fields(fields)
    statement=sqlglot.parse_one(metadata_sql,read='duckdb')
    if type(statement) is not exp.Select:raise ValueError('Metadata projection requires SELECT')
    return statement.select(*(exp.column(name,quoted=True) for name in fields),append=False).sql(dialect='duckdb')
