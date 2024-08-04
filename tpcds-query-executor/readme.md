
Deploy the following config
```json
{
  "name": "lakehouse-metadata-sync1",
  "config": {
    "image": "680330367469.dkr.ecr.eu-central-1.amazonaws.com/iomete/lakehouse-metadata-sync:master-a340926-2021-07-16",
    "main_application_file": "local:///opt/spark/jars/lakehouse-metadata-sync-2.0.0-runner.jar",
		"main_class": "com.iomete.LakehouseMetadataSyncMain",
    "spark_version": "3.1.1",
    "driver": {
      "env_vars": {
        "PRESIDIO_MP_REST_URL": "http://presidio-analyzer-presidio-analyzer.shared-services:80",
        "DATA_CATALOG_SERVICE": "http://data-catalog-staging.iomete-services",
		"APPLICATION_INCLUDE_SCHEMAS": "default"
      }
    }
  }
}
```

## Custom log4j.properties location

You need to add the following java system variables to driver and executors:
```shell
-Dlog4j.configuration=file:///location-to/log4j.properties

#example (Jobs UI)
Java options: -Dlog4j.configuration=file:///etc/iomete/conf/log4j.properties
```
