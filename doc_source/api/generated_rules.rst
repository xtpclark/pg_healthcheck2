Plugin Rule Documentation
=================================

This page is automatically generated from the JSON rule files for all plugins.

Postgres Plugin Rules
~~~~~~~~~~~~~~~~~~~~~

aurora_high_cpu_queries
-----------------------

**Rule #1**

   :Level: ``high``
   :Score: 4
   :Expression: ``float(data.get('cpu_time', 0)) > 1000000000``

**Reasoning:** A query was found with a high cumulative CPU time of {float(data.get('cpu_time', 0)) / 1000000 :.2f} seconds on instance '{data.get('instance_id')}'.

**Recommendations:**

* High CPU consumption can lead to performance degradation and increased costs. Analyze the query's execution plan to identify CPU-intensive operations, such as complex calculations or inefficient joins, and optimize accordingly.

---

aws_cpu_utilization
-------------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``data['metric_name'] == 'CPUUtilization' and isinstance(data['value'], (int, float)) and float(data['value']) > 90``

**Reasoning:** CPU Utilization is critically high at {data['value']:.1f}%.

**Recommendations:**

* Investigate top queries, consider scaling instance class.

---

**Rule #2**

   :Level: ``high``
   :Score: 4
   :Expression: ``data['metric_name'] == 'CPUUtilization' and isinstance(data['value'], (int, float)) and float(data['value']) > 75``

**Reasoning:** CPU Utilization is high at {data['value']:.1f}%.

**Recommendations:**

* Monitor CPU usage and optimize resource-intensive queries.

---

bgwriter_checkpoint_contention
------------------------------

**Rule #1**

   :Level: ``high``
   :Score: 4
   :Expression: ``int(data.get('total_checkpoints', 0)) > 100 and (int(data.get('checkpoints_req', 0)) * 100.0 / int(data.get('total_checkpoints', 1))) > 10``

**Reasoning:** A high percentage ({ (int(data.get('checkpoints_req', 0)) * 100.0 / int(data.get('total_checkpoints', 1))) :.1f}%) of checkpoints are being requested, not timed. This indicates the WAL is filling up too quickly.

**Recommendations:**

* Increase 'max_wal_size' to allow more room for WAL logs between checkpoints. This will smooth out I/O performance, especially during periods of high write activity.

---

low_cache_hit_ratio
-------------------

**Rule #1**

   :Level: ``high``
   :Score: 4
   :Expression: ``float(data.get('hit_ratio_percent', 100)) < 95``

**Reasoning:** The cache hit ratio for database '{data.get('datname')}' is {data.get('hit_ratio_percent')}%, which is below the recommended minimum of 95%. This indicates frequent disk reads.

**Recommendations:**

* A low cache hit ratio is a significant performance bottleneck. Increase the 'shared_buffers' parameter to allocate more memory for caching. Also, analyze slow queries to ensure they are using indexes effectively to reduce unnecessary disk I/O.

---

**Rule #2**

   :Level: ``medium``
   :Score: 3
   :Expression: ``float(data.get('hit_ratio_percent', 100)) < 99``

**Reasoning:** The cache hit ratio for database '{data.get('datname')}' is {data.get('hit_ratio_percent')}%, which is slightly below the ideal of 99% for OLTP workloads.

**Recommendations:**

* While not critical, a cache hit ratio below 99% suggests room for improvement. Monitor 'shared_buffers' and consider a moderate increase if memory is available. Ensure that new, heavy queries are properly indexed.

---

connection_usage
----------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``not settings.get('using_connection_pooler', False) and (int(data['total_connections']) / int(data['max_connections'])) * 100 > 90``

**Reasoning:** Connection usage at {(int(data['total_connections']) / int(data['max_connections'])) * 100:.1f}% of maximum

**Recommendations:**

* Immediate action required: Connection pool near capacity

---

**Rule #2**

   :Level: ``high``
   :Score: 4
   :Expression: ``not settings.get('using_connection_pooler', False) and (int(data['total_connections']) / int(data['max_connections'])) * 100 > 75``

**Reasoning:** Connection usage at {(int(data['total_connections']) / int(data['max_connections'])) * 100:.1f}% of maximum

**Recommendations:**

* Monitor connection usage and consider connection pooling

---

consistently_slow_queries
-------------------------

**Rule #1**

   :Level: ``high``
   :Score: 4
   :Expression: ``float(data.get('mean_exec_time', 0)) > 500``

**Reasoning:** A query was found with a consistently high average execution time of {float(data.get('mean_exec_time', 0)):.2f} ms.

**Recommendations:**

* This query is slow with each execution, indicating a potential issue with its execution plan. Use 'EXPLAIN (ANALYZE, BUFFERS)' on the query to identify inefficiencies like full table scans or poor join strategies.

---

prolonged_lock_waits
--------------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``float(data.get('wait_duration_seconds', 0)) > 300``

**Reasoning:** A session has been blocked for more than 5 minutes ({data.get('wait_duration_seconds')}s), indicating severe transaction contention.

**Recommendations:**

* Investigate the blocking query immediately to understand the cause of the lock. Long-running transactions should be optimized. If necessary, terminate the blocking session using 'SELECT pg_terminate_backend(blocking_pid);' to resolve the contention.

---

duplicate_indexes_found
-----------------------

**Rule #1**

   :Level: ``medium``
   :Score: 3
   :Expression: ``True``

**Reasoning:** Duplicate indexes found on table '{data.get('table_name')}', wasting {data.get('total_wasted_size')}.

**Recommendations:**

* Drop one of the redundant indexes to reduce storage and improve write performance. The redundant indexes are: {data.get('redundant_indexes')}

---

inefficient_temp_file_usage
---------------------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``int(data.get('calls', 0)) > 100 and (float(data.get('total_temp_written', '0 KB').split(' ')[0]) * (1024**2 if 'GB' in data.get('total_temp_written', '') else 1024 if 'MB' in data.get('total_temp_written', '') else 1) / int(data['calls'])) > 10 * 1024``

**Reasoning:** A query is writing an average of { (float(data.get('total_temp_written', '0 KB').split(' ')[0]) * (1024**2 if 'GB' in data.get('total_temp_written', '') else 1024 if 'MB' in data.get('total_temp_written', '') else 1) / int(data['calls'])) / 1024 :.2f} MB of temporary files per execution.

**Recommendations:**

* This query is consistently spilling large amounts of data to disk. This is a strong indicator that 'work_mem' is severely undersized for this query's needs. Use EXPLAIN (ANALYZE, BUFFERS) to confirm the source of the temp file usage and consider increasing 'work_mem'.

---

**Rule #2**

   :Level: ``high``
   :Score: 4
   :Expression: ``int(data.get('calls', 0)) > 100 and (float(data.get('total_temp_written', '0 KB').split(' ')[0]) * (1024**2 if 'GB' in data.get('total_temp_written', '') else 1024 if 'MB' in data.get('total_temp_written', '') else 1) / int(data['calls'])) > 1 * 1024``

**Reasoning:** A query is writing an average of { (float(data.get('total_temp_written', '0 KB').split(' ')[0]) * (1024**2 if 'GB' in data.get('total_temp_written', '') else 1024 if 'MB' in data.get('total_temp_written', '') else 1) / int(data['calls'])) / 1024 :.2f} MB of temporary files per execution.

**Recommendations:**

* This query is frequently spilling to disk. Analyze the query plan for indexing opportunities or consider a moderate increase to 'work_mem' to improve its performance.

---

duplicate_indexes_found
-----------------------

**Rule #1**

   :Level: ``medium``
   :Score: 3
   :Expression: ``True``

**Reasoning:** Duplicate index '{data.get('index_name')}' found on table '{data.get('table_name')}', providing no benefit while adding write overhead.

**Recommendations:**

* Drop the redundant index to reduce storage space and improve the performance of INSERT, UPDATE, and DELETE operations. Keep only one of the duplicate indexes.

---

long_running_queries
--------------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``float(data['total_exec_time']) > 3600000``

**Reasoning:** Query with {float(data['total_exec_time']) / 1000:.1f}s total execution time

**Recommendations:**

* Optimize or terminate long-running queries

---

**Rule #2**

   :Level: ``high``
   :Score: 4
   :Expression: ``float(data['total_exec_time']) > 600000``

**Reasoning:** Query with {float(data['total_exec_time']) / 1000:.1f}s total execution time

**Recommendations:**

* Investigate query performance

---

primary_key_exhaustion_risk
---------------------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``float(data.get('percentage_used', 0)) > 80``

**Reasoning:** Primary key for table '{data.get('table_schema')}.{data.get('table_name')}' is {data.get('percentage_used')}% exhausted.

**Recommendations:**

* Once an integer primary key is exhausted, INSERTs will fail. Plan an immediate migration of this key from 'integer' to 'bigint'.

---

query_workload_concentration
----------------------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``data.get('total_execution_time_all_queries_ms') and data['total_execution_time_all_queries_ms'] > 0 and (sum(q.get('total_exec_time', 0) for q in all_structured_findings.get('query_analysis', {}).get('top_by_time', {}).get('data', [])) / data['total_execution_time_all_queries_ms']) * 100 > 75``

**Reasoning:** High workload concentration detected. The top {settings['row_limit']} queries account for more than 75% of the total database execution time.

**Recommendations:**

* Focus optimization efforts on the top queries from the 'Top Queries by Total Execution Time' section, as this will yield the most significant performance improvements.

---

inactive_replication_slots
--------------------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``int(data.get('inactive_slots_count', 0)) > 0``

**Reasoning:** {data.get('inactive_slots_count')} inactive replication slot(s) were found. Inactive slots prevent the primary from recycling WAL files.

**Recommendations:**

* This is a critical issue that will eventually lead to disk exhaustion and a database outage. Identify and drop any unused replication slots immediately using 'SELECT pg_drop_replication_slot("slot_name");'.

---

weak_password_encryption
------------------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``int(data.get('md5_password_count', 0)) > 0``

**Reasoning:** {data.get('md5_password_count')} user(s) found using MD5 for password encryption, which is vulnerable to offline cracking.

**Recommendations:**

* Migrate all users from MD5 to the more secure 'scram-sha-256' hashing algorithm immediately. Set 'password_encryption = scram-sha-256' in postgresql.conf.

---

superuser_reserved_check
------------------------

**Rule #1**

   :Level: ``warning``
   :Score: 4
   :Expression: ``int(data['superuser_reserved_connections']) < 3``

**Reasoning:** PostgreSQL's 'superuser_reserved_connections' is set to {data['superuser_reserved_connections']}, which is below the recommended minimum of 3. This increases the risk of being locked out of the database during periods of high connection usage.

**Recommendations:**

* Set 'superuser_reserved_connections' to at least 3 (the default value). This can be done via 'ALTER SYSTEM SET superuser_reserved_connections = 3;' followed by a configuration reload ('SELECT pg_reload_conf();').

---

systemic_bloat
--------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``int(data['tables_with_critical_bloat']) > 5``

**Reasoning:** Systemic bloat detected: {data['tables_with_critical_bloat']} tables have critical bloat levels (>50%).

**Recommendations:**

* Global autovacuum settings are likely misconfigured for the workload. Review and tune immediately.

---

**Rule #2**

   :Level: ``high``
   :Score: 4
   :Expression: ``int(data['tables_with_high_bloat']) > 10``

**Reasoning:** Systemic bloat detected: {data['tables_with_high_bloat']} tables have high bloat levels (>20%).

**Recommendations:**

* Global autovacuum settings may need tuning. Investigate workload patterns.

---

tables_without_primary_key
--------------------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``not data.get('is_partition', False) and 'GB' in data.get('table_size', '0 MB') and float(data.get('table_size', '0 MB').split(' ')[0]) > 1``

**Reasoning:** A large table '{data.get('schema_name')}.{data.get('table_name')}' ({data.get('table_size')}) was found without a primary key.

**Recommendations:**

* Tables without primary keys can suffer from poor performance and are incompatible with some replication methods. It is critical to define a primary key for this table to ensure data integrity and performance.

---

**Rule #2**

   :Level: ``medium``
   :Score: 3
   :Expression: ``not data.get('is_partition', False)``

**Reasoning:** Table '{data.get('schema_name')}.{data.get('table_name')}' does not have a primary key.

**Recommendations:**

* All tables should have a primary key to uniquely identify rows. Add a primary key to this table to improve data integrity and query performance.

---

excessive_temp_file_usage
-------------------------

**Rule #1**

   :Level: ``high``
   :Score: 4
   :Expression: ``float(data.get('total_temp_written', '0 MB').split(' ')[0]) > 1024 and 'GB' in data.get('total_temp_written', '0 MB')``

**Reasoning:** A query wrote over 1 GB of temporary files ({data.get('total_temp_written')}), indicating significant memory pressure.

**Recommendations:**

* Queries that spill to disk are often bottlenecked by memory. Increase 'work_mem' for the session, or analyze the query's execution plan to find opportunities for optimization, such as adding indexes.

---

large_unused_indexes
--------------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``int(data.get('index_scans', 1)) == 0 and 'GB' in data.get('index_size', '0 MB') and float(data.get('index_size', '0 MB').split(' ')[0]) > 10``

**Reasoning:** A large, unused index '{data.get('index_name')}' of size {data.get('index_size')} was found. Unused indexes add overhead to write operations and consume significant disk space without any query performance benefit.

**Recommendations:**

* CRITICAL: Verify this index is not used on any read replicas. If it is confirmed to be unused everywhere, drop the index immediately to reclaim space and improve write performance.

---

**Rule #2**

   :Level: ``medium``
   :Score: 3
   :Expression: ``int(data.get('index_scans', 1)) == 0``

**Reasoning:** Potentially unused index '{data.get('index_name')}' was found with zero scans.

**Recommendations:**

* Verify index usage on all read replicas before considering removal. Indexes that appear unused on the primary may be critical for replica query performance.

---

vacuum_bloat
------------

**Rule #1**

   :Level: ``critical``
   :Score: 5
   :Expression: ``int(data['n_live_tup']) > 0 and (int(data['n_dead_tup']) / (int(data['n_dead_tup']) + int(data['n_live_tup']))) > 0.5``

**Reasoning:** Critically high dead tuple ratio in table {data.get('relname', 'N/A')}

**Recommendations:**

* Immediate VACUUM required

---

**Rule #2**

   :Level: ``high``
   :Score: 4
   :Expression: ``int(data['n_live_tup']) > 0 and (int(data['n_dead_tup']) / (int(data['n_dead_tup']) + int(data['n_live_tup']))) > 0.2``

**Reasoning:** High dead tuple ratio in table {data.get('relname', 'N/A')}

**Recommendations:**

* Schedule VACUUM to prevent bloat

---
