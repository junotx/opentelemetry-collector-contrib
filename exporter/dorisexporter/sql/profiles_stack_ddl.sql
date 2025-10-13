CREATE TABLE IF NOT EXISTS %s_stack
(
    service_name                VARCHAR(200),
    stack_hash                  LARGEINT,
    last_seen                   DATETIME(6),
    stack                       ARRAY<STRING>
)
ENGINE = OLAP
UNIQUE KEY(service_name, stack_hash)
DISTRIBUTED BY HASH(service_name, stack_hash) BUCKETS AUTO
%s;
