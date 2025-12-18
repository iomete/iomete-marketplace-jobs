CREATE TABLE iomete_spark_audit_external_table (
    repositoryType INT,
    repositoryName STRING,
    user STRING,
    eventTime STRING,
    accessType STRING,
    resourcePath STRING,
    resourceType STRING,
    action STRING,
    accessResult INT,
    agentId STRING,
    policyId BIGINT,
    resultReason STRING,
    aclEnforcer STRING,
    sessionId STRING,
    clientType STRING,
    clientIP STRING,
    requestData STRING,
    agentHostname STRING,
    logType STRING,
    eventId STRING,
    seqNum BIGINT,
    eventCount BIGINT,
    eventDurationMS BIGINT,
    additionalInfo STRING,
    clusterName STRING,
    zoneName STRING
  )
USING ORC
PARTITIONED BY (day INT)
LOCATION 's3://lakehouse/ranger/audit';