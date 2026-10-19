import gc
import weakref

import numpy as np

from vecstor.compute.vector_execution import TopK


def test_no_vector_projection_does_not_materialize_candidate_lists():
    top=TopK(2,np.zeros(2),{'rerank_ms':0.,'counts':{}},materialize_vector=False)
    top.add([{'id':3},{'id':1},{'id':2}],np.array([[1,0],[1,0],[2,0]],dtype='<f4'))
    assert top.rows==[{'id':1},{'id':3}]
    assert top.distances.tolist()==[1.,1.]


def test_projected_vector_keeps_only_owned_topk_rows():
    vectors=np.zeros((1024,2),dtype='<f4');vectors[:,0]=np.arange(1024)
    reference=weakref.ref(vectors)
    top=TopK(2,np.zeros(2),{'rerank_ms':0.,'counts':{}},materialize_vector=True)
    top.add([{'id':i} for i in range(1024)],vectors)
    del vectors;gc.collect()
    assert reference() is None
    assert len(top.rows)==2
    assert all(isinstance(row['vector'],np.ndarray) for row in top.rows)
    assert top.rows[1]['vector'].tolist()==[1.,0.]
