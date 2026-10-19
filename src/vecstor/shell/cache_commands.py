"""Strict local cache management commands; selected database only."""


def execute_cache(client,args):
    if args not in (['stats'],['clear']):raise ValueError('Usage: .cache stats|clear')
    if not client.database_id:raise ValueError('Select a database first')
    return client.data_cache_stats() if args==['stats'] else client.clear_data_cache()
