CREATE TABLE IF NOT EXISTS %s
(
    service_name                VARCHAR(200),
    timestamp                   DATETIME(6),
    resource_attributes         VARIANT,
    scope_name                  STRING,	
    scope_version               STRING,
    sample_type                 VARCHAR(64),
    sample_unit                 VARCHAR(64),
    period_type                 VARCHAR(64),
    period_unit                 VARCHAR(64),
    sample_value                BIGINT,
    attributes                  VARIANT,
    locations                   ARRAY<STRUCT<level:BIGINT, line:STRING>>,
    stack_trace_id              VARCHAR(128),
    INDEX idx_resource_attributes(resource_attributes) USING INVERTED,
    INDEX idx_attributes(attributes) USING INVERTED
)
ENGINE = OLAP
DUPLICATE KEY(service_name, timestamp)
PARTITION BY RANGE(timestamp) ()
%s;