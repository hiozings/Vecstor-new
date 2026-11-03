from vecstor.rpc.p4_adaptive_acceptance import run


def test_adaptive_lifecycle_exact_only_smoke_is_explicit(tmp_path):
    report=run(tmp_path/'acceptance',rows=1024,dimension=4)
    assert report['passed'] and report['scope']=='exact_only_lifecycle'
    assert report['processes_running_after_cleanup']==[]
    names={row['name'] for row in report['checks']}
    assert {'two_nodes_same_revision','old_lease_keeps_old_vector','write_gap_exact',
            'old_policy_not_reused','compute_restart_no_training','storage_restart_epoch',
            'storage_down_fails','pty_auto'}<=names
    assert all(row['passed'] for row in report['checks'])
