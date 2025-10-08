UNMERGED_PARTS_QUERY = '''
SELECT 
    database, 
    table, 
    name AS part_name, 
    active, 
    bytes_on_disk, 
    modification_time
FROM system.parts
WHERE active = 0 OR modification_time < now() - INTERVAL 1 DAY
ORDER BY modification_time DESC
LIMIT 100
'''
