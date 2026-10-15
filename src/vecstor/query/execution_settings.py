"""Narrow DuckDB AST extension for request-local execution controls."""
from dataclasses import dataclass,replace
from sqlglot import exp
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.tokens import TokenType
from .search_options import SearchOptions


class VecStorDialect(DuckDB):
    class Parser(DuckDB.Parser):
        def _parse_query_modifiers(self,this):
            this=super()._parse_query_modifiers(this)
            if isinstance(this,exp.Select) and self._match(TokenType.SETTINGS):
                if 'execution_settings' in this.meta:self.raise_error('Duplicate SETTINGS')
                values=self._parse_csv(self._parse_equality)
                if not values:self.raise_error('SETTINGS requires assignments')
                this.meta['execution_settings']=values
            return this


def extract_settings(tree):
    nodes=tree.meta.pop('execution_settings',[])
    values={}
    numeric={'gpu_id','nprobe','candidates','max_candidates','max_rounds'}
    for node in nodes:
        if type(node) is not exp.EQ or type(node.this) is not exp.Column or node.this.table:
            raise ValueError('SETTINGS requires name = literal')
        key=node.this.name.lower();value=node.expression
        if key not in SearchOptions.__dataclass_fields__ or key in values:raise ValueError('Unknown or duplicate SETTINGS key')
        if type(value) is not exp.Literal:raise ValueError('SETTINGS requires a literal')
        if key in numeric:
            if value.is_string or not value.is_int:raise ValueError('SETTINGS requires an integer')
            values[key]=int(value.this)
        else:
            if not value.is_string:raise ValueError('SETTINGS requires a string')
            values[key]=value.this
    # Validate individual supplied fields without imposing a session's mode/path yet.
    replace(SearchOptions(mode='approx',path='filter-first' if values.get('mode')=='exact' else 'search-first'),**values).validate()
    return tuple(values.items())


def resolve_execution_options(session,overrides):
    return replace(session,**dict(overrides)).validate()


@dataclass(frozen=True)
class ParsedExecutionSQL:
    sql: str
    overrides: tuple


def parse_execution_settings(sql):
    from .statements import classify_statement
    plan=classify_statement(sql)
    return ParsedExecutionSQL(plan.sql,plan.settings)
