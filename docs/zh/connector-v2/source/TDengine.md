# TDengine

> TDengine(Taos)数据源连接器

## Description

从TDengine(Taos)数据源读取数据，支持全表读取、指定列读取。

## Key features

- [x] [批处理](../../concept/connector-v2-features.md)
- [ ] [流处理](../../concept/connector-v2-features.md)
- [x] [精确一次](../../concept/connector-v2-features.md)
- [x] [并行度](../../concept/connector-v2-features.md)

## 源选项

| 名称        | 类型   | 是否必须	 | 默认值 | 描述                                                                     |
|-------------|--------|-------|---------------|------------------------------------------------------------------------|
| url         | string | 是     | -             | 连接数据库使用的jdbc url，比如: jdbc:TAOS-RS://localhost:6041 |
| username    | string | 是     | -             | 连接数据库使用的用户名                                                            |
| password    | string | 是     | -             | 连接数据库使用的密码                                                             |
| database    | string | 是     |               | 数据源表所在的数据库名称                                                           |
| stable      | string | 是     | -             | 数据源表的名称                                                                |
| fields      | list   | 否     | -             | 需要从数据源表中读取的字段                                                          |
| lower_bound | long   | 是    | -             | 过滤条件。从数据源中读取数据时，数据的最早时间                                                |
| upper_bound | long   | 是    | -             | 过滤条件。从数据源中读取数据时，数据的最晚时间                                                |

## 任务示例

### 从power.meters表中读取ts和longtitude两个字段的数据，指定的数据时间范围是[2018-10-03 14:38:05.000,2018-10-03 14:38:16.800) 

```hocon
source {
        TDengine {
          url : "jdbc:TAOS-RS://localhost:6041/"
          username : "root"
          password : "taosdata"
          database : "power"
          stable : "meters"
          fields : [ts,longtitude]
          lower_bound : "2018-10-03 14:38:05.000"
          upper_bound : "2018-10-03 14:38:16.800"
          result_table_name = "tdengine_result"
        }
}
```

