# Section 3 - Notes

## Joins

### Shuffled Join:

- Joins without a known Partitioner:
- Very expensive
- Spark need to repartition and transfer data all over the shuffles

```elixir
== Physical Plan ==
*(5) Project [id#6L, order#3]
+- *(5) SortMergeJoin [id#6L], [cast(id#2 as bigint)], Inner
   :- *(2) Sort [id#6L ASC NULLS FIRST], false, 0
   :  +- Exchange hashpartitioning(id#6L, 200), true, [id=#27]
   :     +- *(1) Range (1, 100000000, step=1, splits=8)
   +- *(4) Sort [cast(id#2 as bigint) ASC NULLS FIRST], false, 0
      +- Exchange hashpartitioning(cast(id#2 as bigint), 200), true, [id=#33]
         +- *(3) Filter isnotnull(id#2)
            +- *(3) Scan ExistingRDD[id#2,order#3]
```

### Joins with co-partitioned RDDs

- Have the same Partitioner, but maybe different executors
- Usually requires network transfers
- Less expensive than without a know partitioner

```elixir
== Physical Plan ==
*(2) Project [id#6L, order#3]
+- *(2) BroadcastHashJoin [id#6L], [cast(id#2 as bigint)], Inner, BuildRight
   :- *(2) Range (1, 100000000, step=1, splits=8)
   +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint))), [id=#64]
      +- *(1) Filter isnotnull(id#2)
         +- *(1) Scan ExistingRDD[id#2,order#3]
```

### Joins with co-located RDDs

- Have the same Partitioner, executor and share memory
- Can be joined without any network transfers
- Fastest join possible

