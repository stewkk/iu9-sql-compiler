```sh
docker run --rm -it --name some-postgres -e POSTGRES_PASSWORD=mysecretpassword postgres
docker exec -it some-postgres psql -U postgres
```


```sh
git clone https://github.com/brendangregg/FlameGraph.git

sed -i 's|#!/usr/bin/perl -w|#!/usr/bin/env perl|' FlameGraph/stackcollapse-perf.pl
sed -i 's|#!/usr/bin/perl -w|#!/usr/bin/env perl|' FlameGraph/flamegraph.pl

perf record -F 99 -g ./build-release/bin/benchmarks --benchmark_filter="BM_SQL<CachedJitCompiledExpressionExecutor.*"

perf script | FlameGraph/stackcollapse-perf.pl | FlameGraph/flamegraph.pl > benchmarks/flamegraph-jit.svg
```
