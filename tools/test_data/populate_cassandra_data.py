#!/usr/bin/env python3
"""
Populate Cassandra with interesting test data for health check testing.

This script creates various scenarios that will trigger different health checks:
- Multiple keyspaces with different replication strategies
- Tables with data to generate compactions
- Varied table sizes and tombstones
"""

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import uuid
import random
import time

# Configuration
CASSANDRA_HOST = '192.168.1.114'
CASSANDRA_PORT = 9042


def connect_cassandra():
    """Connect to Cassandra cluster."""
    print(f"Connecting to Cassandra at {CASSANDRA_HOST}:{CASSANDRA_PORT}...")
    cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
    session = cluster.connect()
    print("✅ Connected successfully!")
    return cluster, session


def create_keyspaces(session):
    """Create keyspaces with different replication strategies."""
    print("\n=== Creating Keyspaces ===")
    
    # SimpleStrategy (will trigger warnings)
    print("Creating keyspace with SimpleStrategy (will trigger warning)...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS simple_ks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    
    # NetworkTopologyStrategy (recommended)
    print("Creating keyspace with NetworkTopologyStrategy (recommended)...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS network_ks
        WITH replication = {'class': 'NetworkTopologyStrategy', 'datacenter1': 1}
    """)
    
    # Test keyspace (already exists, but ensure it's there)
    print("Ensuring test_keyspace exists...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS test_keyspace
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    
    print("✅ Keyspaces created")


def create_tables(session):
    """Create various tables for testing."""
    print("\n=== Creating Tables ===")
    
    # Table 1: Simple user table
    print("Creating users table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS network_ks.users (
            user_id UUID PRIMARY KEY,
            username TEXT,
            email TEXT,
            created_at TIMESTAMP,
            last_login TIMESTAMP
        )
    """)
    
    # Table 2: Time-series data (will generate tombstones)
    print("Creating sensor_data table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS network_ks.sensor_data (
            sensor_id TEXT,
            timestamp TIMESTAMP,
            temperature DOUBLE,
            humidity DOUBLE,
            PRIMARY KEY (sensor_id, timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC)
    """)
    
    # Table 3: Large dataset table
    print("Creating events table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS network_ks.events (
            event_id UUID PRIMARY KEY,
            event_type TEXT,
            user_id UUID,
            data TEXT,
            timestamp TIMESTAMP
        )
    """)
    
    # Table 4: Simple table in SimpleStrategy keyspace
    print("Creating products table in simple_ks...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS simple_ks.products (
            product_id UUID PRIMARY KEY,
            name TEXT,
            price DECIMAL,
            inventory INT
        )
    """)
    
    print("✅ Tables created")


def populate_users(session, count=1000):
    """Populate users table with test data."""
    print(f"\n=== Populating {count} users ===")
    
    prepared = session.prepare("""
        INSERT INTO network_ks.users (user_id, username, email, created_at, last_login)
        VALUES (?, ?, ?, toTimestamp(now()), toTimestamp(now()))
    """)
    
    for i in range(count):
        session.execute(prepared, (
            uuid.uuid4(),
            f"user_{i}",
            f"user{i}@example.com"
        ))
        
        if (i + 1) % 100 == 0:
            print(f"  Inserted {i + 1}/{count} users...")
    
    print("✅ Users populated")


def populate_sensor_data(session, sensors=10, readings_per_sensor=500):
    """Populate sensor data (will create some tombstones)."""
    from datetime import datetime, timedelta
    
    print(f"\n=== Populating sensor data ({sensors} sensors, {readings_per_sensor} readings each) ===")
    
    prepared_insert = session.prepare("""
        INSERT INTO network_ks.sensor_data (sensor_id, timestamp, temperature, humidity)
        VALUES (?, ?, ?, ?)
    """)
    
    total = sensors * readings_per_sensor
    count = 0
    
    # Start from 30 days ago
    base_time = datetime.now() - timedelta(days=30)
    
    for sensor_id in range(sensors):
        for reading in range(readings_per_sensor):
            # Create timestamp (one reading per hour going back 500 hours)
            timestamp = base_time + timedelta(hours=reading)
            
            session.execute(prepared_insert, (
                f"sensor_{sensor_id:03d}",
                timestamp,
                random.uniform(15.0, 35.0),
                random.uniform(30.0, 80.0)
            ))
            
            count += 1
            if count % 500 == 0:
                print(f"  Inserted {count}/{total} readings...")
    
    # Delete some old data to create tombstones
    print("  Creating tombstones by deleting old data...")
    cutoff_date = datetime.now() - timedelta(days=15)
    
    prepared_delete = session.prepare("""
        DELETE FROM network_ks.sensor_data 
        WHERE sensor_id = ? AND timestamp < ?
    """)
    
    for sensor_id in range(sensors):
        session.execute(prepared_delete, (
            f"sensor_{sensor_id:03d}",
            cutoff_date
        ))
    
    print("✅ Sensor data populated (with tombstones)")


def populate_events(session, count=5000):
    """Populate events table with larger dataset."""
    print(f"\n=== Populating {count} events ===")
    
    event_types = ['login', 'logout', 'purchase', 'view', 'click', 'search']
    
    prepared = session.prepare("""
        INSERT INTO network_ks.events (event_id, event_type, user_id, data, timestamp)
        VALUES (?, ?, ?, ?, toTimestamp(now()))
    """)
    
    for i in range(count):
        session.execute(prepared, (
            uuid.uuid4(),
            random.choice(event_types),
            uuid.uuid4(),
            f"Event data for event {i}"
        ))
        
        if (i + 1) % 500 == 0:
            print(f"  Inserted {i + 1}/{count} events...")
    
    print("✅ Events populated")


def populate_products(session, count=100):
    """Populate products in SimpleStrategy keyspace."""
    print(f"\n=== Populating {count} products (in SimpleStrategy keyspace) ===")
    
    prepared = session.prepare("""
        INSERT INTO simple_ks.products (product_id, name, price, inventory)
        VALUES (?, ?, ?, ?)
    """)
    
    for i in range(count):
        session.execute(prepared, (
            uuid.uuid4(),
            f"Product {i}",
            round(random.uniform(9.99, 999.99), 2),
            random.randint(0, 1000)
        ))
    
    print("✅ Products populated")


def create_multi_node_test_scenarios(session):
    """Create scenarios that benefit from multi-node testing."""
    print("\n=== Creating Multi-Node Test Scenarios ===")
    
    # 1. Keyspace with RF=3 (will be under-replicated on single node)
    print("Creating high replication factor keyspace...")
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS high_rf_ks
        WITH replication = {
            'class': 'NetworkTopologyStrategy', 
            'datacenter1': 3
        }
    """)
    
    # 2. Table that will show token distribution issues
    print("Creating distributed_data table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS high_rf_ks.distributed_data (
            partition_key TEXT PRIMARY KEY,
            data TEXT,
            timestamp TIMESTAMP
        )
    """)
    
    # Insert data across many partitions (will distribute across nodes)
    prepared = session.prepare("""
        INSERT INTO high_rf_ks.distributed_data (partition_key, data, timestamp)
        VALUES (?, ?, toTimestamp(now()))
    """)
    
    for i in range(10000):
        session.execute(prepared, (
            f"partition_{i:05d}",
            f"Data for partition {i}"
        ))
        if (i + 1) % 1000 == 0:
            print(f"  Inserted {i + 1}/10000 partitions...")
    
    # 3. Table with LeveledCompactionStrategy (behaves differently)
    print("Creating leveled_compaction_table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS network_ks.leveled_compaction_table (
            id UUID PRIMARY KEY,
            data TEXT
        ) WITH compaction = {
            'class': 'LeveledCompactionStrategy',
            'sstable_size_in_mb': 160
        }
    """)
    
    # 4. Table with TimeWindowCompactionStrategy (for time-series)
    print("Creating time_series_table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS network_ks.time_series_table (
            sensor_id TEXT,
            hour TIMESTAMP,
            value DOUBLE,
            PRIMARY KEY (sensor_id, hour)
        ) WITH compaction = {
            'class': 'TimeWindowCompactionStrategy',
            'compaction_window_size': 1,
            'compaction_window_unit': 'DAYS'
        }
    """)
    
    print("✅ Multi-node scenarios created")


def create_secondary_indexes(session):
    """Create secondary indexes (some good, some anti-patterns)."""
    print("\n=== Creating Secondary Indexes ===")
    
    # Good index: low cardinality on filtered queries
    print("Creating index on event_type...")
    session.execute("""
        CREATE INDEX IF NOT EXISTS idx_event_type 
        ON network_ks.events (event_type)
    """)
    
    # Anti-pattern: high cardinality (will trigger warning)
    print("Creating index on user_id (high cardinality - anti-pattern)...")
    session.execute("""
        CREATE INDEX IF NOT EXISTS idx_user_id 
        ON network_ks.events (user_id)
    """)
    
    print("✅ Indexes created")


def create_materialized_views(session):
    """Create materialized views for testing."""
    print("\n=== Creating Materialized Views ===")
    
    # MV for users by email
    print("Creating users_by_email materialized view...")
    session.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS network_ks.users_by_email AS
        SELECT user_id, username, email, created_at, last_login
        FROM network_ks.users
        WHERE email IS NOT NULL AND user_id IS NOT NULL
        PRIMARY KEY (email, user_id)
    """)
    
    # MV for events by type - FIXED: timestamp is already in base table's primary key
    print("Creating events_by_type materialized view...")
    session.execute("""
        CREATE MATERIALIZED VIEW IF NOT EXISTS network_ks.events_by_type AS
        SELECT event_id, event_type, user_id, data, timestamp
        FROM network_ks.events
        WHERE event_type IS NOT NULL AND event_id IS NOT NULL
        PRIMARY KEY (event_type, event_id)
    """)
    
    print("✅ Materialized views created")


def create_problematic_scenarios(session):
    """Create scenarios that should trigger health check warnings."""
    print("\n=== Creating Problematic Scenarios ===")
    
    # Empty table (should be flagged for cleanup)
    print("Creating empty_table...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS network_ks.empty_table (
            id UUID PRIMARY KEY,
            data TEXT
        )
    """)
    
    # Table with many tombstones
    print("Creating tombstone_heavy_table and populating...")
    session.execute("""
        CREATE TABLE IF NOT EXISTS network_ks.tombstone_heavy_table (
            id UUID PRIMARY KEY,
            data TEXT,
            deleted_data TEXT
        )
    """)
    
    # Insert and delete to create tombstones
    prepared_insert = session.prepare("""
        INSERT INTO network_ks.tombstone_heavy_table (id, data, deleted_data)
        VALUES (?, ?, ?)
    """)
    
    prepared_delete = session.prepare("""
        DELETE deleted_data FROM network_ks.tombstone_heavy_table WHERE id = ?
    """)
    
    for i in range(1000):
        row_id = uuid.uuid4()
        session.execute(prepared_insert, (row_id, f"Data {i}", f"To be deleted {i}"))
        # Delete the column to create tombstone
        session.execute(prepared_delete, (row_id,))
    
    print("✅ Problematic scenarios created")


def show_summary(session):
    """Show summary of created data."""
    print("\n" + "="*60)
    print("SUMMARY - Data Created")
    print("="*60)
    
    # Keyspaces
    print("\nKeyspaces:")
    rows = session.execute("SELECT keyspace_name, replication FROM system_schema.keyspaces")
    for row in rows:
        if row.keyspace_name not in ['system', 'system_schema', 'system_auth', 'system_traces', 'system_distributed', 'system_views']:
            print(f"  - {row.keyspace_name}: {row.replication}")
    
    # Tables with row counts
    print("\nTables with Data:")
    tables_with_data = [
        ('network_ks', 'users'),
        ('network_ks', 'sensor_data'),
        ('network_ks', 'events'),
        ('simple_ks', 'products'),
        ('high_rf_ks', 'distributed_data'),
        ('network_ks', 'tombstone_heavy_table')
    ]
    
    for keyspace, table in tables_with_data:
        try:
            result = session.execute(f"SELECT COUNT(*) FROM {keyspace}.{table}")
            count = list(result)[0][0]
            print(f"  - {keyspace}.{table}: {count:,} rows")
        except Exception as e:
            print(f"  - {keyspace}.{table}: Error counting - {e}")
    
    # Empty/Schema-only tables
    print("\nSchema-Only Tables (no data):")
    schema_tables = [
        ('network_ks', 'empty_table'),
        ('network_ks', 'leveled_compaction_table'),
        ('network_ks', 'time_series_table')
    ]
    
    for keyspace, table in schema_tables:
        print(f"  - {keyspace}.{table}")
    
    # Secondary Indexes
    print("\nSecondary Indexes:")
    print("  - network_ks.events.idx_event_type (good - low cardinality)")
    print("  - network_ks.events.idx_user_id (anti-pattern - high cardinality)")
    
    # Materialized Views
    print("\nMaterialized Views:")
    print("  - network_ks.users_by_email")
    print("  - network_ks.events_by_type")
    
    print("\n" + "="*60)
    print("✅ Test data population complete!")
    print("="*60)
    print("\nYou can now run health checks that will detect:")
    print("  ⚠️  SimpleStrategy keyspace usage (simple_ks)")
    print("  ⚠️  High replication factor on single node (high_rf_ks RF=3)")
    print("  ✅ NetworkTopologyStrategy keyspace (network_ks)")
    print("  📊 Tables with varying data sizes")
    print("  📊 Multiple compaction strategies (STCS, LCS, TWCS)")
    print("  🗑️  Tombstones in sensor_data and tombstone_heavy_table")
    print("  📈 Compaction opportunities")
    print("  🔍 Secondary index anti-patterns")
    print("  👁️  Materialized views")
    print("  📭 Empty tables (empty_table)")
    print("\nMulti-node features ready (once additional nodes are added):")
    print("  🌐 Token distribution analysis")
    print("  ⚖️  Load balancing across nodes")
    print("  🔄 Replication factor validation")

def main():
    """Main execution."""
    print("="*60)
    print("Cassandra Test Data Population Script")
    print("="*60)
    
    try:
        cluster, session = connect_cassandra()
        
        # Create schema
        create_keyspaces(session)
        create_tables(session)
        
        # NEW: Multi-node scenarios
        create_multi_node_test_scenarios(session)
        create_secondary_indexes(session)
        create_materialized_views(session)
        create_problematic_scenarios(session)
        
        # Populate data
        populate_users(session, count=1000)
        populate_sensor_data(session, sensors=10, readings_per_sensor=500)
        populate_events(session, count=5000)
        populate_products(session, count=100)
        
        # Show summary
        show_summary(session)
        
        # Cleanup
        cluster.shutdown()
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
