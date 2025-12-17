"""
PURE PyFlink DataStream API: MSK to S3 Tables (Iceberg)

This implementation uses ONLY DataStream API by accessing Java Iceberg classes
through PyFlink's Java bridge (Py4J).

Architecture:
- Kafka Source: DataStream API ✅
- Transformations: DataStream API ✅  
- Iceberg Sink: DataStream API via Java bridge ✅

Based on: aws-samples/amazon-managed-service-for-apache-flink-examples
          python/DatastreamKafkaConnector
"""

import json
import logging
import os
from datetime import datetime
from typing import Iterator

from pyflink.common import Types, WatermarkStrategy, Configuration
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction
from pyflink.java_gateway import get_gateway

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
LOG = logging.getLogger(__name__)

# Configuration
APPLICATION_PROPERTIES_FILE_PATH = "/etc/flink/application_properties.json"
IS_LOCAL = os.environ.get("IS_LOCAL", "false").lower() == "true"

if IS_LOCAL:
    APPLICATION_PROPERTIES_FILE_PATH = "application_properties.json"
    CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))


def get_application_properties():
    """Load application properties from JSON file or Runtime Properties"""
    if IS_LOCAL:
        LOG.info(f"Loading local config from {APPLICATION_PROPERTIES_FILE_PATH}")
        with open(APPLICATION_PROPERTIES_FILE_PATH, 'r') as f:
            return json.load(f)
    else:
        LOG.info("Loading Runtime Properties from AWS MSF")
        from com.amazonaws.services.kinesisanalytics.runtime import KinesisAnalyticsRuntime
        
        properties = KinesisAnalyticsRuntime.getApplicationProperties()
        props = {}
        for group_id in properties.keySet():
            props[group_id] = {}
            group_props = properties.get(group_id)
            for key in group_props.keySet():
                props[group_id][key] = group_props.get(key)
        
        return props


class JsonToRowDataMapper(MapFunction):
    """
    MapFunction that converts JSON string to Flink RowData (internal format).
    
    This uses Java classes directly through Py4J to create RowData objects
    that are compatible with Iceberg FlinkSink.
    """
    
    def open(self, runtime_context):
        """Initialize Java objects through gateway"""
        # Get the Java gateway
        gateway = get_gateway()
        
        # Import Java classes for RowData creation
        self.GenericRowData = gateway.jvm.org.apache.flink.table.data.GenericRowData
        self.StringData = gateway.jvm.org.apache.flink.table.data.StringData
        
        LOG.info("JsonToRowDataMapper initialized with Java classes")
    
    def map(self, json_str: str):
        """Parse JSON and convert to RowData"""
        try:
            data = json.loads(json_str)
            metadata = data.get('metadata', {})
            
            # Extract fields
            event_id = data.get('event_id', '')
            user_id = data.get('user_id', '')
            event_type = data.get('event_type', '')
            event_timestamp = data.get('timestamp', 0)
            ride_id = data.get('ride_id', '')
            surge_multiplier = float(metadata.get('surge_multiplier', 0.0))
            estimated_wait_minutes = int(metadata.get('estimated_wait_minutes', 0))
            fare_amount = float(metadata.get('fare_amount', 0.0))
            driver_rating = float(metadata.get('driver_rating', 0.0))
            
            # Calculate event_hour partition
            dt = datetime.fromtimestamp(event_timestamp / 1000.0)
            event_hour = dt.strftime('%Y-%m-%d-%H')
            
            # Create GenericRowData with 10 fields
            row = self.GenericRowData(10)
            
            # Set fields (matching Iceberg schema order)
            row.setField(0, self.StringData.fromString(event_id))
            row.setField(1, self.StringData.fromString(user_id))
            row.setField(2, self.StringData.fromString(event_type))
            row.setField(3, event_timestamp)  # Long
            row.setField(4, self.StringData.fromString(ride_id))
            row.setField(5, surge_multiplier)  # Double
            row.setField(6, estimated_wait_minutes)  # Int
            row.setField(7, fare_amount)  # Double
            row.setField(8, driver_rating)  # Double
            row.setField(9, self.StringData.fromString(event_hour))
            
            LOG.info(f"Converted event_id: {event_id}, event_hour: {event_hour}")
            
            return row
            
        except Exception as e:
            LOG.error(f"Failed to parse JSON: {json_str}, error: {e}")
            return None


def create_iceberg_table_if_not_exists(catalog, table_id, gateway):
    """
    Create Iceberg table using Java API if it doesn't exist.
    
    Args:
        catalog: Java Catalog object
        table_id: Java TableIdentifier object
        gateway: Py4J gateway
    """
    try:
        # Check if table exists
        if catalog.tableExists(table_id):
            LOG.info(f"Table {table_id} already exists")
            return
        
        LOG.info(f"Creating table {table_id}")
        
        # Import Iceberg Java types
        Types = gateway.jvm.org.apache.iceberg.types.Types
        Schema = gateway.jvm.org.apache.iceberg.Schema
        PartitionSpec = gateway.jvm.org.apache.iceberg.PartitionSpec
        
        # Create schema matching your Java implementation exactly
        schema_fields = [
            Types.NestedField.required(1, "event_id", Types.StringType.get()),
            Types.NestedField.required(2, "user_id", Types.StringType.get()),
            Types.NestedField.required(3, "event_type", Types.StringType.get()),
            Types.NestedField.required(4, "event_timestamp", Types.LongType.get()),
            Types.NestedField.required(5, "ride_id", Types.StringType.get()),
            Types.NestedField.required(6, "surge_multiplier", Types.DoubleType.get()),
            Types.NestedField.required(7, "estimated_wait_minutes", Types.IntegerType.get()),
            Types.NestedField.required(8, "fare_amount", Types.DoubleType.get()),
            Types.NestedField.required(9, "driver_rating", Types.DoubleType.get()),
            Types.NestedField.required(10, "event_hour", Types.StringType.get())
        ]
        
        # Create Java array of fields
        field_array = gateway.new_array(gateway.jvm.org.apache.iceberg.types.Types.NestedField, 10)
        for i, field in enumerate(schema_fields):
            field_array[i] = field
        
        # Create schema
        schema = Schema(field_array)
        
        # Create partition spec
        partition_spec = PartitionSpec.builderFor(schema).identity("event_hour").build()
        
        # Create table properties (Java HashMap)
        table_props = gateway.jvm.java.util.HashMap()
        table_props.put("write.format.default", "parquet")
        table_props.put("write.parquet.compression-codec", "snappy")
        
        # Create table
        catalog.createTable(table_id, schema, partition_spec, table_props)
        
        LOG.info(f"✅ Table {table_id} created successfully")
        
    except Exception as e:
        LOG.error(f"Error creating table: {e}", exc_info=True)
        raise


def main():
    # Load configuration
    props = get_application_properties()
    
    # Get configurations
    kafka_config = props.get('KafkaConfig', {})
    bootstrap_servers = kafka_config.get('bootstrap.servers', '')
    topic_name = kafka_config.get('topic', 'user_events')
    group_id = kafka_config.get('group.id', 'flink-s3-tables-datastream')
    
    s3_tables_config = props.get('S3TablesConfig', {})
    warehouse_arn = s3_tables_config.get('warehouse', '')
    namespace = s3_tables_config.get('namespace', 'pocflink')
    table_name = s3_tables_config.get('table', 'ride_events')
    
    LOG.info("="*70)
    LOG.info("🚀 PURE PyFlink DataStream API: MSK to S3 Tables (Iceberg)")
    LOG.info("="*70)
    LOG.info(f"Kafka Bootstrap: {bootstrap_servers}")
    LOG.info(f"Topic: {topic_name}")
    LOG.info(f"S3 Tables Warehouse: {warehouse_arn}")
    LOG.info(f"Target Table: {namespace}.{table_name}")
    LOG.info("="*70)
    
    # Create StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    
    # Configure checkpointing (exactly as in your Java code)
    env.enable_checkpointing(60000)  # 60 seconds
    env.get_checkpoint_config().set_min_pause_between_checkpoints(30000)
    env.get_checkpoint_config().set_checkpoint_timeout(600000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    env.set_parallelism(1)
    
    LOG.info("✅ Checkpointing configured: 60s interval, exactly-once")
    
    # Add JAR dependencies
    if IS_LOCAL:
        jar_path = f"file://{CURRENT_DIR}/target/pyflink-dependencies.jar"
    else:
        jar_path = "file:///opt/flink/lib/pyflink-dependencies.jar"
    
    env.add_jars(jar_path)
    LOG.info(f"✅ Added JAR dependencies from: {jar_path}")
    
    # Get Java gateway for accessing Java classes
    gateway = get_gateway()
    
    # ==================================================================
    # STEP 1: Create Kafka Source using DataStream API
    # ==================================================================
    
    kafka_props = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'AWS_MSK_IAM',
        'sasl.jaas.config': 'software.amazon.msk.auth.iam.IAMLoginModule required;',
        'sasl.client.callback.handler.class': 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'
    }
    
    kafka_source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap_servers) \
        .set_topics(topic_name) \
        .set_group_id(group_id) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_properties(kafka_props) \
        .build()
    
    LOG.info("✅ Kafka source configured with MSK IAM")
    
    # Create DataStream from Kafka
    kafka_stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "MSK Kafka Source",
        type_info=Types.STRING()
    )
    
    # ==================================================================
    # STEP 2: Transform using DataStream API with MapFunction
    # ==================================================================
    
    # Note: We map to Python object first, then convert to Java RowData
    # This is necessary because PyFlink's type system needs a bridge
    parsed_stream = kafka_stream \
        .map(JsonToRowDataMapper()) \
        .filter(lambda x: x is not None) \
        .name("JSON to RowData")
    
    LOG.info("✅ DataStream transformation pipeline configured")
    
    # ==================================================================
    # STEP 3: Create S3 Tables Catalog using Java API
    # ==================================================================
    
    # Import Java classes
    CatalogProperties = gateway.jvm.org.apache.iceberg.CatalogProperties
    Configuration = gateway.jvm.org.apache.hadoop.conf.Configuration
    TableIdentifier = gateway.jvm.org.apache.iceberg.catalog.TableIdentifier
    CatalogLoader = gateway.jvm.org.apache.iceberg.flink.CatalogLoader
    TableLoader = gateway.jvm.org.apache.iceberg.flink.TableLoader
    FlinkSink = gateway.jvm.org.apache.iceberg.flink.sink.FlinkSink
    
    # Create catalog properties (Java HashMap)
    catalog_props = gateway.jvm.java.util.HashMap()
    catalog_props.put(CatalogProperties.CATALOG_IMPL, 
                      "software.amazon.s3tables.iceberg.S3TablesCatalog")
    catalog_props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse_arn)
    
    # Create Hadoop configuration
    hadoop_conf = Configuration()
    
    # Create CatalogLoader
    catalog_loader = CatalogLoader.custom(
        "s3_tables",
        catalog_props,
        hadoop_conf,
        "software.amazon.s3tables.iceberg.S3TablesCatalog"
    )
    
    LOG.info("✅ S3 Tables catalog loader created")
    
    # ==================================================================
    # STEP 4: Create table if not exists
    # ==================================================================
    
    # Load catalog
    catalog = catalog_loader.loadCatalog()
    
    # Create TableIdentifier
    table_id = TableIdentifier.of(namespace, table_name)
    
    # Create table if it doesn't exist
    create_iceberg_table_if_not_exists(catalog, table_id, gateway)
    
    # ==================================================================
    # STEP 5: Create TableLoader and Iceberg Sink using DataStream API
    # ==================================================================
    
    # Create TableLoader
    table_loader = TableLoader.fromCatalog(catalog_loader, table_id)
    table_loader.open()
    
    LOG.info("✅ TableLoader created and opened")
    
    # Get the Java DataStream object (PyFlink wraps Java DataStream)
    java_data_stream = parsed_stream._j_data_stream
    
    # Create FlinkSink using Java API (PURE DATASTREAM!)
    # This is exactly like your Java code: FlinkSink.forRowData(stream)
    sink_builder = FlinkSink.forRowData(java_data_stream)
    sink_builder.tableLoader(table_loader)
    sink_builder.append()
    
    LOG.info("✅ Iceberg DataStream sink configured")
    
    # ==================================================================
    # STEP 6: Execute the job
    # ==================================================================
    
    LOG.info("="*70)
    LOG.info("🎯 Starting Pure DataStream job execution...")
    LOG.info("="*70)
    
    env.execute("Pure DataStream: MSK to S3 Tables")


if __name__ == '__main__':
    main()