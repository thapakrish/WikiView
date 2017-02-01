# Copy files
aws s3 cp FileName s3://kt-wiki/dest

# Delete files recursively
aws s3 rm --recursive s3://kt-wiki/dest-dir

# Sync dirs from local to s3
aws s3 sync source s3://kt-wiki/dest
