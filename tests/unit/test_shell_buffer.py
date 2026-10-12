import pytest
from vecstor.shell.buffer import StatementBuffer

@pytest.mark.parametrize('sql', ["SELECT 'a;''b';", 'SELECT "a;""b";', 'SELECT $$a;b$$;', 'SELECT $tag$a;b$tag$;', '/* outer; /* inner; */ end */ SELECT 1;', '-- x;\nSELECT 1;'])
def test_semicolons_inside_lexical_constructs(sql):
    b = StatementBuffer()
    assert b.feed(sql) == [sql]
    assert b.pending == ''

def test_retains_unfinished_statement_and_cross_chunk_quoting():
    b = StatementBuffer()
    assert b.feed("SELECT 'x;") == []
    assert b.feed("y'; SELECT 2") == ["SELECT 'x;y';"]
    assert b.pending == ' SELECT 2'
    assert b.feed(';') == [' SELECT 2;']

def test_comments_and_empty_statements_never_execute():
    b = StatementBuffer()
    assert b.feed('; /* x; */ ; -- line;\n') == []
    assert not b.pending.strip()
    assert b.feed('/* unfinished') == []
    assert b.pending == '/* unfinished'
    assert b.feed(' */ SELECT 1;') == ['/* unfinished */ SELECT 1;']

def test_limit_preserves_existing_input_and_reset_recovers():
    b = StatementBuffer(max_size=12)
    b.feed('SELECT ')
    with pytest.raises(ValueError):
        b.feed('123456789')
    assert b.pending == 'SELECT '
    b.reset()
    assert b.feed('SELECT 1;') == ['SELECT 1;']

def test_dollar_inside_identifier_is_not_a_quote():
    assert StatementBuffer().feed('SELECT foo$tag$;') == ['SELECT foo$tag$;']


def test_line_comment_stays_comment_when_input_arrives_in_chunks():
    b = StatementBuffer()
    assert b.feed('-- ignored') == []
    assert b.feed('; still ignored\nSELECT 1;') == ['-- ignored; still ignored\nSELECT 1;']
