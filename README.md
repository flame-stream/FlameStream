[![Build Status](https://travis-ci.org/flame-stream/FlameStream.svg?branch=master)](https://travis-ci.org/flame-stream/FlameStream) [![JProfiler](https://www.ej-technologies.com/images/product_banners/jprofiler_small.png)](https://www.ej-technologies.com/products/jprofiler/overview.html)

# FlameStream

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

![roadmap](https://github.com/flame-stream/FlameStream/raw/docs/docs/roadmap.png)

Unlike common models, FlameStream has reduced set of operations: windowed grouping and stateless map, which are enough to implement any stateful pipelines. Such limitation allows achieving strong ordering with low overhead using the lightweight optimistic techniques.
