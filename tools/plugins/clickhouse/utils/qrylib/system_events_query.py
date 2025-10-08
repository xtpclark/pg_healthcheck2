system_events_query = """
SELECT 
    event, 
    value, 
    description
FROM system.events
WHERE event ILIKE '%error%' OR event ILIKE '%warning%'
LIMIT 50
"""
