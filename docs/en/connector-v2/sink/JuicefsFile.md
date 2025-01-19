# JuicefsFile

> Juicefs file sink connector

## Support Those Engines

> Spark<br/>
> Flink<br/>
> SeaTunnel Zeta<br/>

## Usage Dependency

### For Spark/Flink Engine

1. You must ensure your spark/flink cluster already integrated hadoop. The tested hadoop version is 2.x.
2. You must ensure `juicefs-hadoop-x.x.x.jar` in `${SEATUNNEL_HOME}/plugins/` dir.
3. For juicefs hadoop configuration, please refer to [Use JuiceFS on Hadoop Ecosystem](https://juicefs.com/docs/zh/community/hadoop_java_sdk)

### For SeaTunnel Zeta Engine

1. You must ensure `seatunnel-hadoop3-3.1.4-uber.jar`, `juicefs-hadoop-x.x.x.jar` in `${SEATUNNEL_HOME}/lib/` dir.

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [x] [support multiple table write](../../concept/connector-v2-features.md)

By default, we use 2PC commit to ensure `exactly-once`

- [x] file format type
  - [x] text
  - [x] csv
  - [x] parquet
  - [x] orc
  - [x] json
  - [x] excel
  - [x] xml
  - [x] binary

## Data Type Mapping

If write to `csv`, `text` file type, All column will be string.

### Orc File Type

| SeaTunnel Data Type  |     Orc Data Type     |
|----------------------|-----------------------|
| STRING               | STRING                |
| BOOLEAN              | BOOLEAN               |
| TINYINT              | BYTE                  |
| SMALLINT             | SHORT                 |
| INT                  | INT                   |
| BIGINT               | LONG                  |
| FLOAT                | FLOAT                 |
| FLOAT                | FLOAT                 |
| DOUBLE               | DOUBLE                |
| DECIMAL              | DECIMAL               |
| BYTES                | BINARY                |
| DATE                 | DATE                  |
| TIME <br/> TIMESTAMP | TIMESTAMP             |
| ROW                  | STRUCT                |
| NULL                 | UNSUPPORTED DATA TYPE |
| ARRAY                | LIST                  |
| Map                  | Map                   |

### Parquet File Type

| SeaTunnel Data Type  |   Parquet Data Type   |
|----------------------|-----------------------|
| STRING               | STRING                |
| BOOLEAN              | BOOLEAN               |
| TINYINT              | INT_8                 |
| SMALLINT             | INT_16                |
| INT                  | INT32                 |
| BIGINT               | INT64                 |
| FLOAT                | FLOAT                 |
| FLOAT                | FLOAT                 |
| DOUBLE               | DOUBLE                |
| DECIMAL              | DECIMAL               |
| BYTES                | BINARY                |
| DATE                 | DATE                  |
| TIME <br/> TIMESTAMP | TIMESTAMP_MILLIS      |
| ROW                  | GroupType             |
| NULL                 | UNSUPPORTED DATA TYPE |
| ARRAY                | LIST                  |
| Map                  | Map                   |

## Options

| Name                                  |  Type   | Required | Default                                    | Description                                                                                                                            |
|---------------------------------------|---------|----------|--------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| path                                  | string  | yes      | The Juicefs path to write file in.         |                                                                                                                                        |
| tmp_path                              | string  | no       | /tmp/seatunnel                             | The result file will write to a tmp path first and then use `mv` to submit tmp dir to target dir. Need a Juicefs dir.                  |
| jfs_name                              | string  | yes      | -                                          | The jfs name of juicefs file system, for example: `jfs://seatunnel-test`                                                               |
| meta_url                              | string  | yes      | -                                          | The juicefs meta url.                                                                                                                  |
| hadoop_properties                     | Map     | no       | -                                          | Properties passed through to the Hadoop configuration. [Hadoop Configurations](https://juicefs.com/docs/community/hadoop_java_sdk/)    |
| remote_user                           | string  | no       | -                                          | The login user used to connect to hadoop login name. It is intended to be used for remote users in RPC, it won't have any credentials. |
| custom_filename                       | boolean | no       | false                                      | Whether you need custom the filename                                                                                                   |
| file_name_expression                  | string  | no       | "${transactionId}"                         | Only used when custom_filename is true                                                                                                 |
| filename_time_format                  | string  | no       | "yyyy.MM.dd"                               | Only used when custom_filename is true                                                                                                 |
| file_format_type                      | string  | no       | "csv"                                      |                                                                                                                                        |
| field_delimiter                       | string  | no       | '\001'                                     | Only used when file_format_type is text                                                                                                |
| row_delimiter                         | string  | no       | "\n"                                       | Only used when file_format_type is text                                                                                                |
| have_partition                        | boolean | no       | false                                      | Whether you need processing partitions.                                                                                                |
| partition_by                          | array   | no       | -                                          | Only used then have_partition is true                                                                                                  |
| partition_dir_expression              | string  | no       | "${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/" | Only used then have_partition is true                                                                                                  |
| is_partition_field_write_in_file      | boolean | no       | false                                      | Only used then have_partition is true                                                                                                  |
| sink_columns                          | array   | no       |                                            | When this parameter is empty, all fields are sink columns                                                                              |
| is_enable_transaction                 | boolean | no       | true                                       |                                                                                                                                        |
| batch_size                            | int     | no       | 1000000                                    |                                                                                                                                        |
| compress_codec                        | string  | no       | none                                       |                                                                                                                                        |
| common-options                        | object  | no       | -                                          |                                                                                                                                        |
| max_rows_in_memory                    | int     | no       | -                                          | Only used when file_format_type is excel.                                                                                              |
| sheet_name                            | string  | no       | Sheet${Random number}                      | Only used when file_format_type is excel.                                                                                              |
| xml_root_tag                          | string  | no       | RECORDS                                    | Only used when file_format is xml.                                                                                                     |
| xml_row_tag                           | string  | no       | RECORD                                     | Only used when file_format is xml.                                                                                                     |
| xml_use_attr_format                   | boolean | no       | -                                          | Only used when file_format is xml.                                                                                                     |
| parquet_avro_write_timestamp_as_int96 | boolean | no       | false                                      | Only used when file_format is parquet.                                                                                                 |
| parquet_avro_write_fixed_as_int96     | array   | no       | -                                          | Only used when file_format is parquet.                                                                                                 |
| encoding                              | string  | no       | "UTF-8"                                    | Only used when file_format_type is json,text,csv,xml.                                                                                  |

### path [string]

The target dir path is required.

### bucket [string]

The bucket address of Juicefs file system, for example: `Juicefs://tyrantlucifer-image-bed`

### access_key [string]

The access key of Juicefs file system.

### access_secret [string]

The access secret of Juicefs file system.

### endpoint [string]

The endpoint of Juicefs file system.

### custom_filename [boolean]

Whether custom the filename

### file_name_expression [string]

Only used when `custom_filename` is `true`

`file_name_expression` describes the file expression which will be created into the `path`. We can add the variable `${now}` or `${uuid}` in the `file_name_expression`, like `test_${uuid}_${now}`,
`${now}` represents the current time, and its format can be defined by specifying the option `filename_time_format`.

Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.

### filename_time_format [String]

Only used when `custom_filename` is `true`

When the format in the `file_name_expression` parameter is `xxxx-${Now}` , `filename_time_format` can specify the time format of the path, and the default value is `yyyy.MM.dd` . The commonly used time formats are listed as follows:

| Symbol |    Description     |
|--------|--------------------|
| y      | Year               |
| M      | Month              |
| d      | Day of month       |
| H      | Hour in day (0-23) |
| m      | Minute in hour     |
| s      | Second in minute   |

### file_format_type [string]

We supported as the following file types:

`text` `csv` `parquet` `orc` `json` `excel` `xml` `binary`

Please note that, The final file name will end with the file_format_type's suffix, the suffix of the text file is `txt`.

### field_delimiter [string]

The separator between columns in a row of data. Only needed by `text` file format.

### row_delimiter [string]

The separator between rows in a file. Only needed by `text` file format.

### have_partition [boolean]

Whether you need processing partitions.

### partition_by [array]

Only used when `have_partition` is `true`.

Partition data based on selected fields.

### partition_dir_expression [string]

Only used when `have_partition` is `true`.

If the `partition_by` is specified, we will generate the corresponding partition directory based on the partition information, and the final file will be placed in the partition directory.

Default `partition_dir_expression` is `${k0}=${v0}/${k1}=${v1}/.../${kn}=${vn}/`. `k0` is the first partition field and `v0` is the value of the first partition field.

### is_partition_field_write_in_file [boolean]

Only used when `have_partition` is `true`.

If `is_partition_field_write_in_file` is `true`, the partition field and the value of it will be write into data file.

For example, if you want to write a Hive Data File, Its value should be `false`.

### sink_columns [array]

Which columns need be written to file, default value is all the columns get from `Transform` or `Source`.
The order of the fields determines the order in which the file is actually written.

### is_enable_transaction [boolean]

If `is_enable_transaction` is true, we will ensure that data will not be lost or duplicated when it is written to the target directory.

Please note that, If `is_enable_transaction` is `true`, we will auto add `${transactionId}_` in the head of the file.

Only support `true` now.

### batch_size [int]

The maximum number of rows in a file. For SeaTunnel Engine, the number of lines in the file is determined by `batch_size` and `checkpoint.interval` jointly decide. If the value of `checkpoint.interval` is large enough, sink writer will write rows in a file until the rows in the file larger than `batch_size`. If `checkpoint.interval` is small, the sink writer will create a new file when a new checkpoint trigger.

### compress_codec [string]

The compress codec of files and the details that supported as the following shown:

- txt: `lzo` `none`
- json: `lzo` `none`
- csv: `lzo` `none`
- orc: `lzo` `snappy` `lz4` `zlib` `none`
- parquet: `lzo` `snappy` `lz4` `gzip` `brotli` `zstd` `none`

Tips: excel type does not support any compression format

### common options

Sink plugin common parameters, please refer to [Sink Common Options](../sink-common-options.md) for details.

### max_rows_in_memory [int]

When File Format is Excel,The maximum number of data items that can be cached in the memory.

### sheet_name [string]

Writer the sheet of the workbook

### xml_root_tag [string]

Specifies the tag name of the root element within the XML file.

### xml_row_tag [string]

Specifies the tag name of the data rows within the XML file.

### xml_use_attr_format [boolean]

Specifies Whether to process data using the tag attribute format.

### parquet_avro_write_timestamp_as_int96 [boolean]

Support writing Parquet INT96 from a timestamp, only valid for parquet files.

### parquet_avro_write_fixed_as_int96 [array]

Support writing Parquet INT96 from a 12-byte field, only valid for parquet files.

### encoding [string]

Only used when file_format_type is json,text,csv,xml.
The encoding of the file to write. This param will be parsed by `Charset.forName(encoding)`.

## How to Create an Juicefs Data Synchronization Jobs

The following example demonstrates how to create a data synchronization job that reads data from Fake Source and writes it to the Juicefs:

For text file format with `have_partition` and `custom_filename` and `sink_columns`

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to product data
source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

# write data to Juicefs
sink {
  JuicefsFile {
    path="/seatunnel/sink"
    jfs_name = "jfs://sealtunneltest"
    meta_url = "mysql://xxxx:xxx@(127.0.0.1:3306)/juicefs"
    hadoop_properties = {
      juicefs.cache-size = 0
    }
    file_format_type = "text"
    field_delimiter = "\t"
    row_delimiter = "\n"
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    custom_filename = true
    file_name_expression = "${transactionId}_${now}"
    filename_time_format = "yyyy.MM.dd"
    sink_columns = ["name","age"]
    is_enable_transaction = true
  }
}
```

For parquet file format with `have_partition` and `sink_columns`

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to product data
source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

# Write data to Juicefs
sink {
  JuicefsFile {
    path = "/seatunnel/sink"
    jfs_name = "jfs://sealtunneltest"
    meta_url = "mysql://xxxx:xxx@(127.0.0.1:3306)/juicefs"
    hadoop_properties = {
      juicefs.cache-size = 0
    }
    have_partition = true
    partition_by = ["age"]
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    file_format_type = "parquet"
    sink_columns = ["name","age"]
  }
}
```

For orc file format simple config

```bash
# Set the basic configuration of the task to be performed
env {
  parallelism = 1
  job.mode = "BATCH"
}

# Create a source to product data
source {
  FakeSource {
    schema = {
      fields {
        name = string
        age = int
      }
    }
  }
}

# Write data to Juicefs
sink {
  JuicefsFile {
    path="/seatunnel/sink"
    jfs_name = "jfs://sealtunneltest"
    meta_url = "mysql://xxxx:xxx@(127.0.0.1:3306)/juicefs"
    hadoop_properties = {
      juicefs.cache-size = 0
    }
    file_format_type = "orc"
  }
}
```

### Multiple Table

For extract source metadata from upstream, you can use `${database_name}`, `${table_name}` and `${schema_name}` in the path.

```bash

env {
  parallelism = 1
  spark.app.name = "SeaTunnel"
  spark.executor.instances = 2
  spark.executor.cores = 1
  spark.executor.memory = "1g"
  spark.master = local
  job.mode = "BATCH"
}

source {
  FakeSource {
    tables_configs = [
       {
        schema = {
          table = "fake1"
          fields {
            c_map = "map<string, string>"
            c_array = "array<int>"
            c_string = string
            c_boolean = boolean
            c_tinyint = tinyint
            c_smallint = smallint
            c_int = int
            c_bigint = bigint
            c_float = float
            c_double = double
            c_bytes = bytes
            c_date = date
            c_decimal = "decimal(38, 18)"
            c_timestamp = timestamp
            c_row = {
              c_map = "map<string, string>"
              c_array = "array<int>"
              c_string = string
              c_boolean = boolean
              c_tinyint = tinyint
              c_smallint = smallint
              c_int = int
              c_bigint = bigint
              c_float = float
              c_double = double
              c_bytes = bytes
              c_date = date
              c_decimal = "decimal(38, 18)"
              c_timestamp = timestamp
            }
          }
        }
       },
       {
       schema = {
         table = "fake2"
         fields {
           c_map = "map<string, string>"
           c_array = "array<int>"
           c_string = string
           c_boolean = boolean
           c_tinyint = tinyint
           c_smallint = smallint
           c_int = int
           c_bigint = bigint
           c_float = float
           c_double = double
           c_bytes = bytes
           c_date = date
           c_decimal = "decimal(38, 18)"
           c_timestamp = timestamp
           c_row = {
             c_map = "map<string, string>"
             c_array = "array<int>"
             c_string = string
             c_boolean = boolean
             c_tinyint = tinyint
             c_smallint = smallint
             c_int = int
             c_bigint = bigint
             c_float = float
             c_double = double
             c_bytes = bytes
             c_date = date
             c_decimal = "decimal(38, 18)"
             c_timestamp = timestamp
           }
         }
       }
      }
    ]
  }
}

sink {
  JuicefsFile {
    jfs_name = "jfs://sealtunneltest"
    meta_url = "mysql://xxxx:xxx@(127.0.0.1:3306)/juicefs"
    hadoop_properties = {
      juicefs.cache-size = 0
    }
    path = "/tmp/fake_empty/text/${table_name}"
    row_delimiter = "\n"
    partition_dir_expression = "${k0}=${v0}"
    is_partition_field_write_in_file = true
    file_name_expression = "${transactionId}_${now}"
    file_format_type = "text"
    filename_time_format = "yyyy.MM.dd"
    is_enable_transaction = true
    compress_codec = "lzo"
  }
}
```

## Changelog

### 2.4.9-beta 2024-10-31

- Add Juicefs Sink Connector

### Tips

> 1.[SeaTunnel Deployment Document](../../start-v2/locally/deployment.md).

