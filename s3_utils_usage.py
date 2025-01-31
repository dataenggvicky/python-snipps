# Init

import s3_utils

s3 = S3Utils('my-bucket', 'us-west-2')

# Basic operations
content = s3.read_object('path/file.txt')
s3.write_object('new/file.txt', content, content_type='text/plain')
files = s3.list_files(prefix='path/')
size = s3.get_file_size('path/file.txt')

# Copy/Move operations
s3.copy_object('source-bucket', 'old/path.txt', 'new/path.txt', 'dest-bucket')
s3.move_object('source-bucket', 'old/path.txt', 'new/path.txt', 'dest-bucket')

# Copy with custom args
s3.copy_object(
    'source-bucket', 'old/path.txt', 'new/path.txt',
    extra_args={'StorageClass': 'STANDARD_IA'}
)