Experiments with Apache Beam
---

Investigate the watermark mechanism for a custom `UnboundedSource` implementation.

Late data is not dropped as expected when using a custom `UnboundedSource`.

Setup: Use a fixed window size of 5 seconds. The following sequence of values with the given timestamp offset if fed in:

| value | timestamp offset |
| ---| --- |
| 0 | 0 |
| 1 | 1 |
| 2 | 2 |
| 3 | **6** |
| 4 | 4 |
| 5 | 5 |
| 6 | 6 |
| 7 | 7 |
| 8 | **11** |
| 9 | 9 |

Note that the elements with value 3 and 8 have a timestamp offset that moves them into the next window and causes the next values (4 and 9) to be late.

Summing the values in their windows should result in

| window | sum |
| --- | ---
| 0 | 0 + 1 + 2 = 3 |
| 1 | 3 + 5 + 6 + 7 = 21 |
| 2 | 8 |

When feeding in the values with a `org.apache.beam.sdk.testing.TestStream` the result of running the program is as expected:

```
res: KV{[1974-10-03T02:40:00.000Z..1974-10-03T02:40:05.000Z), 3}
res: KV{[1974-10-03T02:40:05.000Z..1974-10-03T02:40:10.000Z), 21}
res: KV{[1974-10-03T02:40:10.000Z..1974-10-03T02:40:15.000Z), 8}
state: DONE
counter: MetricResult{key=Combine.globally(Anonymous)/Combine.perKey(Anonymous)/GroupByKey/GroupAlsoByWindow:org.apache.beam.runners.direct.GroupAlsoByWindowEvaluatorFactory$GroupAlsoByWindowEvaluator:DroppedDueToLateness, committedOrNull=2, attempted=2}
counter: MetricResult{key=PAssert$0/VerifyAssertions/ParDo(DefaultConclude)/ParMultiDo(DefaultConclude):org.apache.beam.sdk.testing.PAssert:PAssertSuccess, committedOrNull=1, attempted=1}
```
Note that the `DroppedDueToLateness` counter has the value 2.

When running the same pipeline with values read from a custom `org.apache.beam.sdk.io.UnboundedSource` implementation the result is:

```
res: KV{[1974-10-03T02:40:00.000Z..1974-10-03T02:40:05.000Z), 7}
res: KV{[1974-10-03T02:40:05.000Z..1974-10-03T02:40:10.000Z), 30}
res: KV{[1974-10-03T02:40:10.000Z..1974-10-03T02:40:15.000Z), 8}
state: DONE
counter: MetricResult{key=PAssert$0/VerifyAssertions/ParDo(DefaultConclude)/ParMultiDo(DefaultConclude):org.apache.beam.sdk.testing.PAssert:PAssertSuccess, committedOrNull=1, attempted=1}
```

Apparently late data (4 and 9) is not dropped. The calculated window sums are:

| window | sum |
| --- | --- |
| 0 | 0 + 1 + 2 + 4 = 7 |
| 1 | 3 + 5 + 6 + 7 + 9 = 30 |
| 2 | 8 |
