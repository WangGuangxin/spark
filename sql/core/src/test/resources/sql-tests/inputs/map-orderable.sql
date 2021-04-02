--CONFIG_DIM2 spark.sql.codegen.wholeStage=true
--CONFIG_DIM2 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=CODEGEN_ONLY
--CONFIG_DIM2 spark.sql.codegen.wholeStage=false,spark.sql.codegen.factoryMode=NO_CODEGEN

CREATE OR REPLACE TEMPORARY VIEW t AS SELECT * FROM VALUES
  (map('a', 1, 'b', 2, 'c', 3)),
  (map('c', 3, 'b', 2, 'a', 1)),
  (map('d', 4)),
  (map('a', 1, 'e', 2)),
  (map('d', 4)),
  (map('d', 5))
  AS v(m);

CREATE OR REPLACE TEMPORARY VIEW t2 AS SELECT * FROM VALUES
  (map('b', 2, 'a', 1, 'c', 3))
  AS v(m2);

CREATE OR REPLACE TEMPORARY VIEW t3 AS SELECT * FROM VALUES
  (map(map('a', 1, 'b', 2), map('hello', array('i', 'j')))),
  (map(map('b', 1, 'a', 1), map('world', array('o', 'p')))),
  (map(map('b', 2, 'a', 1), map('hello', array('m', 'n')))),
  (map(map('b', 2, 'a', 1), map('hello', array('i', 'j'))))
  AS v(nested_map);

-- group by
select m, count(1) from t group by m;

-- group by
select m2, count(1), percentile(id, 0.5) from (
  select
    case when size(m) == 3 then m else map('b', 2, 'a', 1, 'c', 3)
    end as m2,
    1 as id
  from t
)
group by m2;

-- distinct
select distinct m from t;

-- distinct multiple columns
select distinct m, m2 from t join t2 on t.m = t2.m2;

-- order by
select m from t order by m;

-- window
select m, count(1) over (partition by m) from t;

-- join
select m, m2 from t join t2 on t.m = t2.m2;

-- filter by map
select m from t where m = map('b', 2, 'a', 1, 'c', 3);

-- group by map with nested type
select nested_map, count(1) from t3 group by nested_map;

-- distinct map with nested type
select distinct nested_map from t3;

-- order by map with nested type
select nested_map from t3 order by nested_map;