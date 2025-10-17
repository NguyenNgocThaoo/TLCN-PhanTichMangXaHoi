-- ===========================================================
--  CREATE USERS & DATABASES FOR NESSSIE + AIRFLOW (PostgreSQL)
-- ===========================================================

-- 1. Tạo user airflow (nếu chưa có)
DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow'
   ) THEN
      CREATE ROLE airflow LOGIN PASSWORD 'airflow';
   END IF;
END
$$;

-- 2. Tạo database nessie nếu chưa tồn tại
\connect postgres
DO
$$
DECLARE
   _db_exists BOOLEAN;
BEGIN
   SELECT EXISTS(SELECT FROM pg_database WHERE datname = 'nessie') INTO _db_exists;
   IF NOT _db_exists THEN
      RAISE NOTICE 'Creating database nessie...';
   END IF;
END
$$;
-- Vì không thể chạy CREATE DATABASE trong DO, ta chạy trực tiếp (nếu chưa có)
\if :{?PGDATABASE}
\endif
\echo Checking for database nessie...
\! psql -U root -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='nessie'" | grep -q 1 || psql -U root -d postgres -c "CREATE DATABASE nessie OWNER root;"

-- 3. Tạo database airflow nếu chưa tồn tại
\echo Checking for database airflow...
\! psql -U root -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='airflow'" | grep -q 1 || psql -U root -d postgres -c "CREATE DATABASE airflow OWNER airflow;"

-- 4. Gán quyền cho airflow
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
