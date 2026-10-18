"""Test-only process crash points. This is never exposed by the service CLI."""
import argparse
import os
from pathlib import Path
import signal

parser=argparse.ArgumentParser()
parser.add_argument('--root',required=True)
parser.add_argument('--point',required=True)
parser.add_argument('--marker',required=True)
args=parser.parse_args()


def crash():
    with Path(args.marker).open('w') as target:
        target.write(args.point);target.flush();os.fsync(target.fileno())
    os.kill(os.getpid(),signal.SIGKILL)


from vecstor.storage.index_catalog import IndexCatalog
from vecstor.storage.index_artifacts import ArtifactStore
if args.point in ('before_commit','after_commit'):
    original=IndexCatalog.commit
    def commit(self,*values,**kwargs):
        if args.point=='before_commit':crash()
        result=original(self,*values,**kwargs)
        crash()
        return result
    IndexCatalog.commit=commit
elif args.point=='upload_before_seal':
    ArtifactStore.seal=lambda *a,**k:crash()
elif args.point=='validate_before_install':
    ArtifactStore.validate_and_install=lambda *a,**k:crash()
elif args.point=='gc_before_delete':
    ArtifactStore.delete_retired=lambda *a,**k:crash()
else:raise ValueError('Unknown test crash point')

from vecstor.rpc.server import main
raise SystemExit(main(['storage','--data-root',args.root,'--listen','127.0.0.1:0']))
