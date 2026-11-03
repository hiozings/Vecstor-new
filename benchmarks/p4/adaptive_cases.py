"""Explicit adaptive workload scope; large exact matrices are opt-in."""


def case(selectivity=1., k=10, distribution='independent', capacity_mib=64, *, rows=100000,
         dimension=128, samples=200, exact_samples=5, concurrency=1, delay_ms=None,
         patterns=('cold','warm')):
    return dict(rows=rows,dimension=dimension,distribution=distribution,selectivity=selectivity,k=k,
        capacity_mib=capacity_mib,samples=samples,exact_samples=exact_samples,concurrency=concurrency,
        delay_ms=delay_ms,patterns=list(patterns))


def matrix_cases(tier):
    if tier=='smoke':return [case(rows=1024,dimension=4,samples=2,exact_samples=2)]
    if tier=='representative':
        result=[case(s,k) for s,k in ((.001,10),(.01,10),(.1,10),(.5,10),(1.,10),(1.,100))]
        return result+[case(.001,10,'correlated'),case(.1,100,'correlated'),case(1.,100,capacity_mib=8)]
    if tier=='correctness':
        return [case(s,k,d,rows=n,samples=2,exact_samples=2)
            for n in (10000,100000) for d in ('independent','correlated')
            for s in (.001,.01,.1,.5,1.) for k in (10,100)]
    if tier=='extended':
        return [case(s,k,samples=20,concurrency=c,delay_ms=delay,
                     patterns=('cold','warm','independent','hotset'))
            for s,k in ((.01,10),(1.,100)) for c in (1,4,8) for delay in (None,0.,1.,5.)]
    raise ValueError('Unknown adaptive matrix tier')
