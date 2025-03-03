DROP TABLE IF EXISTS `timpani_qa`.`content_items`;

CREATE EXTERNAL TABLE IF NOT EXISTS `timpani_qa`.`content_items` (
  `run_id` string COMMENT 'id of run that pulled content',
  `workspace_id` string COMMENT 'slug id workspace config',
  `source_id` string COMMENT 'slug id of content source',
  `query_id` string COMMENT 'slug id for query of workspace',
  `page_id` string COMMENT 'id/index of chunk within query',
  `created_at` timestamp COMMENT 'timestamp that content was acquired by timpani',
  `content_id` string COMMENT 'content id (usually from content source)',
  `content` string COMMENT 'json formatted content from source'
) COMMENT "database connection to content_items partition of timpani raw store QA s3 bucket"
PARTITIONED BY (date_id integer)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://timpani-raw-store-qa/content_items'
TBLPROPERTIES ('classification' = 'json');

MSCK REPAIR TABLE content_items;
