import runpy


def test_warm_benchmark_keeps_connections_and_separates_client_control(tmp_path):
    cal=runpy.run_path('benchmarks/gpu/calibrate.py')
    cal['run'](tmp_path/'cal',sizes=[1024],dimension=3,distributions=['independent'],calibration_count=3,evaluation_count=5,selectivities=[1.],ks=[10])
    module=runpy.run_path('benchmarks/gpu/sessions.py')
    r=module['run'](tmp_path/'cal',tmp_path/'sessions',selected=[(1024,'independent',1.,10)],variants=['uncached','cached'],samples=4,repeats=1,concurrencies=[1])
    assert r['passed'] and len(r['groups'])==4
    for g in r['groups']:
        assert g['client_control_calls']==(4 if g['variant']=='uncached' else 0)
        assert g['connections_created']==1 and g['queries']==4
        assert g['p50_ms']>0


def test_fused_variant_preserves_answers_and_reduces_storage_rpc(tmp_path):
    cal=runpy.run_path('benchmarks/gpu/calibrate.py')
    cal['run'](tmp_path/'cal',sizes=[1024],dimension=3,distributions=['independent'],calibration_count=3,evaluation_count=5,selectivities=[1.],ks=[10])
    module=runpy.run_path('benchmarks/gpu/sessions.py')
    r=module['run'](tmp_path/'cal',tmp_path/'sessions',selected=[(1024,'independent',1.,10)],variants=['cached','fused'],samples=4,repeats=1,concurrencies=[1])
    for device in ('cpu','gpu'):
        before=next(g for g in r['groups'] if g['device']==device and g['variant']=='cached')
        after=next(g for g in r['groups'] if g['device']==device and g['variant']=='fused')
        assert after['median_storage_rpc']==before['median_storage_rpc']-1


def test_query_can_select_separate_reads_on_fused_enabled_compute(tmp_path):
    from contextlib import ExitStack
    from vecstor.datasets import generate_dataset,save_dataset
    from vecstor.rpc.acceptance import ServiceProcess
    from vecstor.rpc.sql_client import SessionClient
    root=tmp_path/'storage';save_dataset(generate_dataset(1024,3),root/'imports/a.npz')
    with ExitStack() as cleanup:
        storage=ServiceProcess(['storage','--data-root',str(root),'--listen','127.0.0.1:0'],tmp_path/'storage.log');cleanup.callback(storage.stop)
        compute=ServiceProcess(['compute','--storage',storage.address,'--listen','127.0.0.1:0','--fused-candidates'],tmp_path/'compute.log');cleanup.callback(compute.stop)
        with SessionClient(compute.address) as c:
            c.open_database('demo',True);c.import_dataset('a.npz','items');c.build_index('items',nlist=1,device='gpu')
            sql="SELECT id FROM items ORDER BY vector_distance(vector,[0,0,0]) LIMIT 10 SETTINGS device='gpu',mode='approx',path='search-first'"
            a=c.execute(sql+",candidate_data_path='fused'")
            b=c.execute(sql+",candidate_data_path='separate'")
            assert a['rows']==b['rows']
            assert a['execution_plan']['candidate_data_path']=='fused' and b['execution_plan']['candidate_data_path']=='separate'
            assert b['execution_plan']['counts']['rpc_calls']==a['execution_plan']['counts']['rpc_calls']+1
