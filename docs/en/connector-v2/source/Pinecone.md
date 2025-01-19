# Pinecone

> Pinecone source connector

## Description

This Pinecone source connector reads data from Pinecone
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

| Name       | Type    | Required | Default | Description                        |
|------------|---------|----------|---------|------------------------------------|
| api_key    | String  | Yes      | -       | Pinecone token for authentication. |
| index      | String  | Yes      | -       | Pinecone index name                |
| batch_size | Integer | No       | 100     | Batch size to read                 |

## Task Example

```bash
source {
  Milvus {
    token = "*****"
    index = "movie_reviews"
  }
}
```

## Changelog

