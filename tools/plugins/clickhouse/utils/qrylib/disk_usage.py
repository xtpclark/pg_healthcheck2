QUERY_DISK_USAGE = '''
SELECT 
    name,
    path,
    formatReadableSize(total_space) AS total_space_readable,
    formatReadableSize(free_space) AS free_space_readable,
    total_space,
    free_space,
    round((free_space / total_space) * 100, 2) AS free_space_percent
FROM system.disks
ORDER BY free_space_percent ASC
'''
