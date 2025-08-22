

#!/bin/bash
set -e
set -u

# Create additional databases if POSTGRES_MULTIPLE_DATABASES is defined
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
  echo "Creating multiple databases: $POSTGRES_MULTIPLE_DATABASES"

  for db in $(echo $POSTGRES_MULTIPLE_DATABASES | tr ',' ' '); do
    echo "Creating database '$db' with owner '$POSTGRES_USER'"
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
      CREATE DATABASE $db OWNER $POSTGRES_USER;
EOSQL
  done
  echo "Multiple databases created successfully!"
fi
