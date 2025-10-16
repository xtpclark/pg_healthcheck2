# lib/sanitizer.py
"""Input validation and sanitization."""

def sanitize_user_input(user_query):
    """
    Validates and sanitizes user input.
    
    Args:
        user_query: Raw user input string
        
    Returns:
        str: Sanitized input
        
    Raises:
        ValueError: If input is invalid or suspicious
    """
    if len(user_query) > 1000:
        raise ValueError("Query too long (max 1000 characters)")

    # Check for obvious injection patterns
    dangerous = ['rm -rf', 'DROP TABLE', '; --', '$(', '`']
    for pattern in dangerous:
        if pattern in user_query:
            raise ValueError(f"Query contains suspicious pattern: {pattern}")

    return user_query.strip()
