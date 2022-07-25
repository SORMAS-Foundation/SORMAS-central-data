export PGPASSWORD=postgres
cat continent.sql | psql  --host=localhost --port=5434 -U postgres -d sormas_db
cat subcontinent.sql | psql  --host=localhost --port=5434 -U postgres -d sormas_db
cat country.sql | psql  --host=localhost --port=5434 -U postgres -d sormas_db
cat region.sql | psql  --host=localhost --port=5434 -U postgres -d sormas_db
cat district.sql | psql  --host=localhost --port=5434 -U postgres -d sormas_db
cat community.sql | psql  --host=localhost --port=5434 -U postgres -d sormas_db