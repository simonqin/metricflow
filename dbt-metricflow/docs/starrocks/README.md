# StarRocks 接入（dbt-metricflow）

## 适用版本
- StarRocks 3.x

## 安装
```bash
pip install "dbt-metricflow[starrocks]"
```

## profiles.yml 最小示例
```yaml
metricflow_testing:
  target: dev
  outputs:
    dev:
      type: starrocks
      host: <host>
      port: 9030
      user: <user>
      password: <password>
      database: <database>
      schema: <schema>
```

## 环境变量示例（可选）
```bash
export DBT_ENV_SECRET_HOST=<host>
export DBT_PROFILE_PORT=9030
export DBT_ENV_SECRET_USER=<user>
export DBT_ENV_SECRET_PASSWORD=<password>
export DBT_ENV_SECRET_DATABASE=<database>
export DBT_ENV_SECRET_SCHEMA=<schema>
```

## 最小命令示例
```bash
dbt debug
dbt run
mf query --metrics transactions --group-by metric_time --order metric_time
```
