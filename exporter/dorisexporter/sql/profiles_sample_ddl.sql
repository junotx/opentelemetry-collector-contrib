CREATE TABLE IF NOT EXISTS %s_sample
(
    service_name                VARCHAR(200),
    timestamp                   DATETIME(6),
    attributes                  VARIANT,
    profile_type                VARCHAR(330),
    sample_type                 VARCHAR(64),
    sample_unit                 VARCHAR(64),
    period_type                 VARCHAR(64),
    period_unit                 VARCHAR(64),
    period                      BIGINT,
    sample_value                BIGINT,
    stack_hash                  LARGEINT,
    INDEX idx_attributes(attributes) USING INVERTED
)
ENGINE = OLAP
DUPLICATE KEY(service_name, timestamp)
PARTITION BY RANGE(timestamp) ()
%s;
