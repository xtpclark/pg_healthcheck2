SELECT setmetric('AllowedUserLogins','t'),
       setmetric('ActiveOnly','t'),
       setmetric('AdminOnly','f'),
       setmetric('Any','t'),
       setmetric('filesystem_key_path','/some/secure/path/asciitext.key'),
       setmetric('aws_kms_key_arn','arn:aws:kms:us-east-1:123456789012:key/your-kms-key-id');
