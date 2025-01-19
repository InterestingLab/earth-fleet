# Tencent VectorDB

> Tencent VectorDB source connector

## Description

This Tencent VectorDB source connector reads data from Tencent VectorDB
## Key Features

- [x] [batch](../../concept/connector-v2-features.md)
- [x] [exactly-once](../../concept/connector-v2-features.md)
- [ ] [column projection](../../concept/connector-v2-features.md)

## Data Type Mapping

| Pinecone Data Type  | SeaTunnel Data Type |
|---------------------|---------------------|
| FLOAT_VECTOR        | FLOAT_VECTOR        |
| SPARSE_FLOAT_VECTOR | SPARSE_FLOAT_VECTOR |

## Source Options

| Name       | Type    | Required | Default | Description                 |
|------------|---------|----------|---------|-----------------------------|
| url        | String  | Yes      | -       | endpoint                    |
| user_name  | String  | Yes      | -       | user name                   |
| api_key    | String  | Yes      | -       | api key for authentication. |
| database   | String  | Yes      | -       | database                    |
| collection | String  | Yes      | -       | collection name             |
| batch_size | Integer | No       | 100     | Batch size to read          |

## Task Example

```bash
source {
  TencentVectorDB {
    url = "*****"
    user_name = "root"
    api_key = "*****"
    database = "default"
    collection = "movie_reviews"
  }
}
```

## Changelog
