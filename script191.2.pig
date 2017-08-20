A = LOAD 'hbase://customer' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('details:*', '-loadKey true') as (id:INT,details:MAP[]);

B = FOREACH A GENERATE id, (CHARARRAY)details#'name' as name, (CHARARRAY)details#'location' as location, (INT)details#'age' as age;

C = GROUP B all;

D = FOREACH C GENERATE MAX(B.age) as max_age;

E = FILTER B BY age == (int)D.max_age;

dump E;

F = FOREACH C GENERATE MIN(B.age) as min_age;

G = FILTER B BY age == (int)F.min_age;

dump G;

