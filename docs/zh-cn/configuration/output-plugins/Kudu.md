## Output plugin : Kudu

* Author: InterestingLab
* Homepage: https://interestinglab.github.io/waterdrop
* Version: 1.0.0

### Description

将数据写入kudu

### Options

| name | type | required | default value |
| --- | --- | --- | --- |
| [kudu_master](#kudu_master-string) | string | yes | - |
| [kudu_table](#kudu_table) | string | yes | - |
| [mode](#mode-string) | string | no | insert |


##### kudu_master [string]

kudu的master，多个master以逗号隔开

##### kudu_table [string]

kudu中要写入的表名,表必须已经存在

##### mode [string]

写入kudu中采取的模式,支持 insert|update|upsert|insertIgnore,默认为insert
insert和insertIgnore :insert在遇见主键冲突将会报错，insertIgnore不会报错，将会舍弃这条数据
update和upsert :update找不到要更新的主键将会报错，upsert不会，将会把这条数据插入


### Example

```
kudu{
   kudu_master="hadoop01:7051,hadoop02:7051,hadoop03:7051"
   kudu_table="my_kudu_table"
   mode="upsert"
 }
```
