"""Explicit full and user-approved representative experiment manifests."""
from itertools import product


def case(rows=10000,distribution='independent',selectivity=1.,k=10,mode='approx',path='search-first',
         concurrency=1,samples=200,patterns=('cold','warm','independent','hotset'),capacity_mib=64,
         delay_ms=None,device='cpu',category='costs',variant_policy='all',dimension=128):
    return dict(rows=rows,distribution=distribution,selectivity=selectivity,k=k,mode=mode,path=path,
        concurrency=concurrency,samples=samples,patterns=list(patterns),capacity_mib=capacity_mib,
        delay_ms=delay_ms,device=device,category=category,variant_policy=variant_policy,dimension=dimension)


def variants(spec):
    if spec['variant_policy']=='cache-only':return [('off','projected','separate'),('on','projected','separate')]
    paths=('separate','fused') if spec['mode']=='approx' and spec['path']=='search-first' else ('separate',)
    return list(product(('off','on'),('full','projected'),paths))


def matrix_cases(tier):
    if tier=='smoke':return [case(rows=1024,dimension=4,samples=2,patterns=('cold','warm'),category='smoke')]
    if tier=='correctness':
        return [case(n,d,s,k,m,p,samples=200,patterns=('cold','warm'),category='correctness')
            for n,d,s,k,(m,p) in product((10000,100000),('independent','correlated'),(.001,.01,.1,.5,1.),(10,100),
                (('exact','filter-first'),('approx','filter-first'),('approx','search-first')))]
    if tier=='costs':
        result=[case(n,d,s,k,concurrency=c) for n,d,(s,k),c in product((10000,100000),('independent','correlated'),((.001,10),(.1,10),(1.,100)),(1,4,8))]
        result += [case(rows=100000,k=100,capacity_mib=8,category='capacity',variant_policy='cache-only')]
        result += [case(rows=100000,k=100,delay_ms=d,category='latency') for d in (0,1,5)]
        return result
    if tier=='gpu':return [case(rows=100000,selectivity=s,k=k,concurrency=c,device='gpu',category='gpu') for (s,k),c in product(((1.,100),(.1,10)),(1,4,8))]
    if tier=='representative':
        result=[case(n,d,concurrency=c,samples=20) for n,d,c in product((10000,100000),('independent','correlated'),(1,4,8))]
        result += [case(n,d,s,k,m,p,samples=1,patterns=('cold','warm'),category='correctness-probe')
            for n,d,(s,k),(m,p) in product((10000,100000),('independent','correlated'),((.001,10),(1.,100)),(('exact','filter-first'),('approx','filter-first')))]
        result += [case(rows=100000,selectivity=s,k=k,samples=20,patterns=('cold','warm'),category='selectivity') for s,k in ((.001,10),(.1,10),(1.,100))]
        result += [case(rows=100000,k=100,samples=200,patterns=('cold','warm'),capacity_mib=c,category='capacity',variant_policy='cache-only') for c in (8,64)]
        result += [case(rows=100000,samples=20,patterns=('cold','warm'),delay_ms=d,category='latency',variant_policy='cache-only') for d in (0,1,5)]
        result += [case(rows=100000,selectivity=s,k=k,samples=20,patterns=('cold','warm'),device='gpu',category='gpu') for s,k in ((1.,100),(.1,10))]
        return result
    raise ValueError('Unknown data read experiment tier')
