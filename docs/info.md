# Hive Partitioning
Hive partitioning is a partitioning strategy that is used to split a table into multiple files based on partition keys. The files are organized into folders. Within each folder, the partition key has a value that is determined by the name of the folder.

Below is an example of a Hive partitioned file hierarchy. The files are partitioned on two keys (year and month).
climate_ts/partitioned/model_name=WRF-NARR_HIS/grid_name=R10C29/data_0.parquet
climate_ts
├── model_name=WRF-NARR_HIS
│    ├── grid_name=R10C29
│    │   ├── data_0.parquet
│    └── grid_name=R10C30
│        └── data_0.parquet
└── model_name=bcc-csm1.1_RCP85_PREC_6km
     ├── grid_name=R10C29
     │   ├── data_0.parquet
     │   └── data_0.parquet
     └── grid_name=R10C29
         └── data_0.parquet

Files stored in this hierarchy can be read using the hive_partitioning flag.

SELECT *
FROM read_parquet('climate_ts/*/*/*.parquet', hive_partitioning = true);

When we specify the hive_partitioning flag, the values of the columns will be read from the directories.

## Filter Pushdown
Filters on the partition keys are automatically pushed down into the files. This way the system skips reading files that are not necessary to answer a query. For example, consider the following query on the above dataset:

SELECT *
FROM read_parquet('climate_ts/*/*/*.parquet', hive_partitioning = true)
WHERE model_name = WRF-NARR_HIS
  AND grid_name = R10C29


## Autodetection
By default the system tries to infer if the provided files are in a hive partitioned hierarchy. And if so, the hive_partitioning flag is enabled automatically. The autodetection will look at the names of the folders and search for a 'key' = 'value' pattern. This behavior can be overridden by using the hive_partitioning configuration option:

SET hive_partitioning = false;

## Hive Types
hive_types is a way to specify the logical types of the hive partitions in a struct:

SELECT *
FROM read_parquet(
    'dir/**/*.parquet',
    hive_partitioning = true,
    hive_types = {'release': DATE, 'orders': BIGINT}
);

hive_types will be autodetected for the following types: DATE, TIMESTAMP and BIGINT. To switch off the autodetection, the flag hive_types_autocast = 0 can be set.