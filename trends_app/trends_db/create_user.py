from werkzeug.security import generate_password_hash
import getpass

def main():
    """
    Generates a SQL INSERT statement for a new user with a securely hashed password.
    """
    print("--- Create a New Web UI User ---")
    
    # Get user details from input
    username = input("Enter username: ")
    company_id = int(input("Enter company_id (e.g., 1 for the default company): "))
    password = getpass.getpass("Enter password: ")
    confirm_password = getpass.getpass("Confirm password: ")

    if password != confirm_password:
        print("\n❌ Passwords do not match. Aborting.")
        return

    # Generate the secure password hash
    password_hash = generate_password_hash(password)

    # Print the SQL statement to be executed
    print("\n" + "="*80)
    print("✅ Success! Copy and run the following SQL against your trend database:")
    print("="*80)
    print(f"""
INSERT INTO users (username, password_hash, company_id, password_change_required)
VALUES ('{username}', '{password_hash}', {company_id}, FALSE);
""")
    print("="*80)

if __name__ == '__main__':
    main()
