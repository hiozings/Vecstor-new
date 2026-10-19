from dataclasses import replace

from test_data_cache_execution import cached,SQL


def test_gpu_cached_projected_fused_and_cpu_fallback(cached):
    cached.compute.fused_candidates=True;cached.compute.metadata_projection='projected'
    cached.client.load_index('items',device='gpu')
    cached.client.search_options=replace(cached.client.search_options,device='gpu',candidate_data_path='fused',device_fallback='cpu')
    cold=cached.client.execute(SQL);warm=cached.client.execute(SQL)
    assert warm['rows']==cold['rows']
    assert warm['execution_plan']['actual_device']=='gpu'
    assert warm['execution_plan']['counts']['vectors_fetched']==0
    cached.client.clear_indexes('items',device='gpu')
    cpu=cached.client.execute(SQL)
    assert cpu['rows']==warm['rows']
    assert cpu['execution_plan']['actual_device']=='cpu'
    assert cpu['execution_plan']['artifact_id']==warm['execution_plan']['artifact_id']
    assert cpu['execution_plan']['data_cache']['vectors_served_from_cache']==64
