[![Build Status](https://travis-ci.org/flame-stream/FlameStream.svg?branch=master)](https://travis-ci.org/flame-stream/FlameStream) [![JProfiler](https://www.ej-technologies.com/images/product_banners/jprofiler_small.png)](https://www.ej-technologies.com/products/jprofiler/overview.html)

# FlameStream

## Overview

FlameStream is a distributed stream processing model, that has the following properties:

- _Pure streaming_: records are processed one at a time
- _Deterministic_: results are determined only by input and not changed between independent runs
- _Consistency_: model provides for exactly-once semantics

The distributed implementation of FlameStream model is written in Java and uses the Akka Actors framework for messaging.

The implementation is based on the following grounds:

- Determinism is achieved via strong ordering
- Idempotence via determinism
- Exactly-once via idempotence

Our road to exactly-once in comparison with other approaches:

![roadmap](https://raw.githubusercontent.com/flame-stream/FlameStream/master/docs/roadmap.png)

Unlike common models, FlameStream has reduced set of operations: windowed grouping and stateless map, which are enough to implement any stateful pipelines. Such limitation allows achieving strong ordering with low overhead using the lightweight optimistic techniques.

## Benchmarking

See details in [benchmarking readme](https://github.com/flame-stream/FlameStream/blob/master/benchmark/README.md).

## Papers
- [	Igor E. Kuralenok, Artem Trofimov, Nikita Marshalkin, Boris Novikov:
 *Deterministic Model for Distributed Speculative Stream Processing.* **ADBIS 2018**](https://link.springer.com/chapter/10.1007/978-3-319-98398-1_16)
- [	Igor E. Kuralenok, Artem Trofimov, Nikita Marshalkin, Boris Novikov:
 *FlameStream: Model and Runtime for Distributed Stream Processing.* **BeyondMR@SIGMOD 2018**](https://dl.acm.org/citation.cfm?id=3209273)
 - [	Igor E. Kuralenok, Nikita Marshalkin, Artem Trofimov, Boris Novikov:
 *An optimistic approach to handle out-of-order events within analytical stream processing.* **SEIM 2018**](http://ceur-ws.org/Vol-2135/SEIM_2018_paper_16.pdf)
