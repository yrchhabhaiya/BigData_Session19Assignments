create external table customer_hive
(
id INT,
name STRING,
location STRING,
age INT
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
with serdeproperties ("hbase.columns.mapping"=":key, details:name, details:location, details:age")
tblproperties("hbase.table.name"="customer");

select MIN(age), MAX(age) from customer_hive;

