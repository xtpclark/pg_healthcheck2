-- Migration: Standardize report action privileges
-- Date: 2025-11-14
-- Description: Add consistent privilege structure for View/Edit/Download/Delete actions

\echo '=== Standardizing Report Action Privileges ==='

-- Core report action privileges
\echo 'Creating core report privileges...'
SELECT createpriv('reports', 'ViewReports', 'Can view any report type');
SELECT createpriv('reports', 'EditReports', 'Can edit report metadata (title, description)');
SELECT createpriv('reports', 'DownloadReports', 'Can download/export reports to PDF/DOCX/JSON');
SELECT createpriv('reports', 'DeleteReports', 'Can delete reports');
SELECT createpriv('reports', 'ShareReports', 'Can share reports with other users');

-- Consolidate AI report generation (remove duplicate)
\echo 'Consolidating AI report privileges...'
DELETE FROM priv WHERE priv_name = 'GenerateReport' AND priv_module = 'ai-report';
-- Keep GenerateReports, rename to GenerateAIReports for clarity
UPDATE priv SET priv_name = 'GenerateAIReports'
WHERE priv_name = 'GenerateReports' AND priv_module = 'ai-report';
SELECT createpriv('ai-report', 'RegenerateReports', 'Can regenerate existing AI reports with new prompts');

-- Technology-specific edit privileges
\echo 'Creating technology-specific edit privileges...'
SELECT createpriv('TRENDS', 'EditPostgreSQLReports', 'Can edit PostgreSQL health check reports');
SELECT createpriv('TRENDS', 'EditKafkaReports', 'Can edit Kafka health check reports');
SELECT createpriv('TRENDS', 'EditCassandraReports', 'Can edit Cassandra health check reports');
SELECT createpriv('TRENDS', 'EditOpenSearchReports', 'Can edit OpenSearch health check reports');
SELECT createpriv('TRENDS', 'EditClickHouseReports', 'Can edit ClickHouse health check reports');
SELECT createpriv('TRENDS', 'EditMongoDBReports', 'Can edit MongoDB health check reports');
SELECT createpriv('TRENDS', 'EditAllTechnologyReports', 'Can edit reports for all database technologies');

-- ClickHouse view privileges (missing)
\echo 'Adding ClickHouse view privileges...'
SELECT createpriv('TRENDS', 'ViewClickHouseAnalysis', 'Can view ClickHouse analysis views and reports');
SELECT createpriv('TRENDS', 'ViewClickHouseHealthTrends', 'Can view ClickHouse cluster health trends');

-- Deprecated: Remove old EditReports from report-history module
\echo 'Cleaning up deprecated privileges...'
DELETE FROM priv WHERE priv_name = 'EditReports' AND priv_module = 'report-history';

-- Grant download privilege to all existing users (default: view = download)
\echo 'Granting download privilege to existing users...'
INSERT INTO usrpriv (usrpriv_username, usrpriv_priv_id)
SELECT DISTINCT u.username, p.priv_id
FROM users u
CROSS JOIN priv p
WHERE p.priv_name = 'DownloadReports'
  AND NOT EXISTS (
      SELECT 1 FROM usrpriv up2
      WHERE up2.usrpriv_username = u.username
        AND up2.usrpriv_priv_id = p.priv_id
  );

\echo ''
\echo '=== Privilege Migration Complete ==='
\echo ''
\echo 'New privileges added:'
SELECT priv_module, priv_name, priv_descrip
FROM priv
WHERE priv_module IN ('reports', 'ai-report', 'TRENDS')
  AND (priv_name LIKE '%Edit%' OR priv_name LIKE '%Download%' OR priv_name LIKE '%ClickHouse%' OR priv_name LIKE '%Share%' OR priv_name LIKE '%Delete%')
ORDER BY priv_module, priv_name;
