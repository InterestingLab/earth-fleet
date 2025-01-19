# TDengine

> TDengine(Taos)数据接收器

## Description

连接TDengine(Taos)数据库并将数据写入数据库

***需要注意的点***

+ 写入tdengine时无需指定子表名称，会将reader中读取出来的第一个字段当作子表名称，假如数据源是一个jdbc任务，query语句是select device_id,ts,power,tenant_id from xxx，那么写入的子表名称将会是device_id的值。
+ 同理，写入时会将读出来的最后几列当作tag，假设写入的超级表指定了tag是tenant_id，那么在写入时会将tenant_id的值当作tag

## Key features

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)

## 源选项

| 名称       | 类型     | 是否必须	 | 默认值 | 描述                                                 |
|----------|--------|-------|---------------|----------------------------------------------------|
| url      | string | 是     | -             | 连接数据库使用的jdbc url，比如: jdbc:TAOS-RS://localhost:6041 |
| username | string | 是     | -             | 连接数据库使用的用户名                                        |
| password | string | 是     | -             | 连接数据库使用的密码                                         |
| database | string | 是     |               | 数据写入的数据库名称                                         |
| stable   | string | 是     | -             | 数据写入的数据表名称                                         |
| fields   | list   | 否     | -             | 写入的数据表字段                                           |
| timezone | string | 否     | -             | 客户端时区                                              | 

## 任务示例

### 往power.meters表中写入ts和longtitude两个字段的数据

power.meters表结构:

```sql
CREATE STABLE `meter` (`ts` TIMESTAMP, `latitude` DOUBLE, `longtitude` DOUBLE) TAGS (`tenant_id` INT)
```
seatunnel 配置信息
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
查询tdengine数据库查看最新数据

```sql
taos> select * from meter;
           ts            |         latitude          |        longtitude         |  tenant_id  |
================================================================================================
 2024-03-08 03:47:54.000 |    9.594743198982019e+306 |                      NULL |  1215981593 |
 2024-08-18 07:34:56.000 |    1.712320739538011e+308 |                      NULL |  1965348204 |
 2024-07-05 21:59:04.000 |    4.499682197436227e+307 |                      NULL |   203469706 |
```
