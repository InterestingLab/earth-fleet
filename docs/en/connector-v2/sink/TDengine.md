# TDengine

> TDengine sink connector

## Description

Used to write data to TDengine. You need to create stable before running seatunnel task

***attention please***
+ you don't need to config sub-table name, first column read is used as sub-table name
+ we will use last n columns as table tags, n is count of tags

## Key features

- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [cdc](../../concept/connector-v2-features.md)

## Options

| name     | type   | required | default value |
|----------|--------|----------|---------------|
| url      | string | yes      | -             |
| username | string | yes      | -             |
| password | string | yes      | -             |
| database | string | yes      |               |
| stable   | string | yes      | -             |
| fields   | list   | no       | -             |
| timezone | string | no       | UTC           |

### url [string]

the url of the TDengine when you select the TDengine

e.g.

```
jdbc:TAOS-RS://localhost:6041/
```

### username [string]

the username of the TDengine when you select

### password [string]

the password of the TDengine when you select

### database [string]

the database of the TDengine when you select

### stable [string]

the stable of the TDengine when you select

### timezone [string]

the timeznoe of the TDengine sever, it's important to the ts field

## Example

### write 3 columns into table power.meter 

power.meter schema information:

```sql
CREATE STABLE `meter` (`ts` TIMESTAMP, `latitude` DOUBLE, `longtitude` DOUBLE) TAGS (`tenant_id` INT)
```
seatunnel config file

```hocon
source {
  # This is a example input plugin **only for test and demonstrate the feature input plugin**
  FakeSource {
    row.num = 1
    schema = {
      fields {
        table_name = int
        ts = timestamp
        latitude = double
        tenant_id = int
      }
    }
    result_table_name = "fake"
  }
}

sink {
    TDengine {
        source_table_name='fake'
        url : "jdbc:TAOS-RS://localhost:6041/"
        username : "root"
        password : "taosdata"
        database : "test"
        fields: ['ts','latitude']
        stable : "meter"
        timezone: UTC
    }
}
```
execute query and print table data
```sql
taos> select * from meter;
           ts            |         latitude          |        longtitude         |  tenant_id  |
================================================================================================
 2024-03-08 03:47:54.000 |    9.594743198982019e+306 |                      NULL |  1215981593 |
 2024-08-18 07:34:56.000 |    1.712320739538011e+308 |                      NULL |  1965348204 |
 2024-07-05 21:59:04.000 |    4.499682197436227e+307 |                      NULL |   203469706 |

