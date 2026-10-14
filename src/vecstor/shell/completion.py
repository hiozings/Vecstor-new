"""Session-local completion; never performs RPC while typing."""
from prompt_toolkit.completion import WordCompleter


class SchemaCompleter(WordCompleter):
    def __init__(self):
        self.keywords = frozenset('EXPLAIN ANALYZE SELECT FROM WHERE ORDER BY LIMIT INSERT INTO VALUES UPDATE SET DELETE CREATE TABLE DROP GROUP HAVING COUNT SUM AVG MIN MAX vector_distance .help .databases .open .datasets .import .tables .schema .status .mode .timer .search .index .plan .clear .quit'.split())
        self.names = set(self.keywords)
        super().__init__(sorted(self.names), ignore_case=True)

    def replace(self, result):
        """Replace all schema names after a full schema refresh or database switch."""
        self.names = set(self.keywords)
        self.remember(result)

    def remember(self, result):
        """Merge an explicit table/schema command result into the current cache."""
        if isinstance(result, dict):
            columns = [str(column.get('name', '') if isinstance(column, dict) else column)
                       for column in result.get('columns', [])]
            # DESCRIBE rows are [table, column, type, nullable, key]. SHOW
            # TABLES uses [name]. Neither result's header is a user field.
            indices = [index for index, name in enumerate(columns) if name in ('table', 'column', 'name')]
            for row in result.get('rows', []):
                for index in indices:
                    if index < len(row) and isinstance(row[index], str):
                        self.names.add(row[index])
        self.words = sorted(self.names)
