import datetime
import json
import os
import sys

from dateutil.parser import parse
from kubernetes import client, config
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType


def create_spark_session():
    """Create and return a Spark session"""
    return (
        SparkSession.builder.appName('KubernetesEventCollector')
        .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions')
        .config('spark.iomete.catalogUpdates.enabled', 'false')
        .getOrCreate()
    )


def get_last_scrape_time(spark, s3_bucket, state_path):
    """Get the last scrape time from state file in S3"""
    state_file_path = f's3a://{s3_bucket}/{state_path}'

    try:
        state_df = spark.read.json(state_file_path)

        if not state_df.isEmpty():
            last_scrape_time = state_df.first()['last_scrape_time']
            return parse(last_scrape_time)
        else:
            # File exists but is empty
            return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=15)

    except Exception as e:
        print(f'State file not found or error reading it: {e}')
        # Default to 15 minutes ago, if file not found or error reading it
        return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes=15)


def save_last_scrape_time(spark, s3_bucket, state_path, last_scrape_time):
    """Save the last scrape time to state file in S3"""
    state_file_path = f's3a://{s3_bucket}/{state_path}'

    # Create DataFrame with state info
    state_data = [{'last_scrape_time': last_scrape_time.isoformat()}]
    state_df = spark.createDataFrame(state_data)

    # Write state to S3
    state_df.coalesce(1).write.mode('overwrite').json(state_file_path)


def get_kubernetes_events(target_namespaces, since_time):
    """Get Kubernetes events from specified namespaces since the specified time"""
    try:
        try:
            # Try in-cluster config first (for when running inside k8s)
            config.load_incluster_config()
        except config.ConfigException:
            # Fall back to kubeconfig file
            config.load_kube_config()

        v1 = client.CoreV1Api()

        events = []

        # If target_namespaces is empty, get events from all namespaces
        if not target_namespaces:
            # Fail fast if no namespaces are provided
            print('No target namespaces provided. Set TARGET_NAMESPACES environment variable.')
            sys.exit(1)  # Exit with error code
        else:
            # Get events for each specified namespace
            for namespace in target_namespaces:
                continue_token = None
                while True:
                    # Handle pagination with continue token
                    api_response = v1.list_namespaced_event(namespace=namespace, limit=500, _continue=continue_token)

                    # Process events
                    for event in api_response.items:
                        event_time = event.last_timestamp or event.event_time
                        if event_time and event_time >= since_time:
                            events.append(create_event_dict(event))

                    # Check if we need to continue pagination
                    continue_token = api_response.metadata._continue
                    if not continue_token:
                        break

        return events
    except Exception as e:
        print(f'Error getting Kubernetes events: {e}')
        return []


def create_event_dict(event):
    """Convert a Kubernetes event to a dictionary"""
    return {
        'uid': event.metadata.uid,
        'name': event.metadata.name,
        'namespace': event.metadata.namespace,
        'event_time': event.event_time.isoformat() if event.event_time else None,
        'first_timestamp': event.first_timestamp.isoformat() if event.first_timestamp else None,
        'last_timestamp': event.last_timestamp.isoformat() if event.last_timestamp else None,
        'type': event.type,
        'reason': event.reason,
        'message': event.message,
        'source_component': event.source.component if event.source else None,
        'source_host': event.source.host if event.source else None,
        'involved_object_kind': event.involved_object.kind,
        'involved_object_name': event.involved_object.name,
        'involved_object_namespace': event.involved_object.namespace,
        'involved_object_uid': event.involved_object.uid,
        'count': event.count,
        'reporting_component': event.reporting_component,
        'reporting_instance': event.reporting_instance,
        # Add a date field to use for partitioning
        'event_date': (event.last_timestamp or event.event_time or datetime.datetime.now(datetime.timezone.utc))
        .date()
        .isoformat(),
    }


def main():
    # S3 configuration
    s3_bucket = os.environ.get('S3_BUCKET', 'k8s-events-bucket')
    events_path = os.environ.get('EVENTS_PATH', 'k8s/events')
    state_path = os.environ.get('STATE_PATH', 'k8s/_STATE')

    # Get target namespaces from environment variable (comma-separated list)
    namespace_list = os.environ.get('TARGET_NAMESPACES', '')
    target_namespaces = [ns.strip() for ns in namespace_list.split(',') if ns.strip()]

    # Initialize Spark
    spark = create_spark_session()

    # Get last scrape time
    last_scrape_time = get_last_scrape_time(spark, s3_bucket, state_path)
    print(f'Getting events since: {last_scrape_time}')
    print(f"Target namespaces: {target_namespaces or 'Not provided'}")

    # Get current time (to be saved as new last scrape time)
    current_time = datetime.datetime.now(datetime.timezone.utc)

    # Get Kubernetes events
    events = get_kubernetes_events(target_namespaces, last_scrape_time)
    print(f'Found {len(events)} new events')

    if events:
        # Define schema for events
        schema = StructType(
            [
                StructField('uid', StringType(), True),
                StructField('name', StringType(), True),
                StructField('namespace', StringType(), True),
                StructField('event_time', StringType(), True),
                StructField('first_timestamp', StringType(), True),
                StructField('last_timestamp', StringType(), True),
                StructField('type', StringType(), True),
                StructField('reason', StringType(), True),
                StructField('message', StringType(), True),
                StructField('source_component', StringType(), True),
                StructField('source_host', StringType(), True),
                StructField('involved_object_kind', StringType(), True),
                StructField('involved_object_name', StringType(), True),
                StructField('involved_object_namespace', StringType(), True),
                StructField('involved_object_uid', StringType(), True),
                StructField('count', IntegerType(), True),
                StructField('reporting_component', StringType(), True),
                StructField('reporting_instance', StringType(), True),
                StructField('event_date', StringType(), True),
            ]
        )

        # Create DataFrame
        events_df = spark.createDataFrame(events, schema)

        # Convert string date to date type for partitioning
        events_df = events_df.withColumn('partition_date', to_date(col('event_date')))

        # Generate output path
        output_path = f's3a://{s3_bucket}/{events_path}'

        # Write events to S3 in Parquet format with partitioning by namespace and date
        events_df.write.partitionBy('namespace', 'partition_date').mode('append').parquet(output_path)
        print(f'Events written to {output_path}')

    # Save new last scrape time
    save_last_scrape_time(spark, s3_bucket, state_path, current_time)
    print(f'Updated last scrape time to: {current_time}')


if __name__ == '__main__':
    main()
