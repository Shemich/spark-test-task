-- file: 10-create-user-and-db.sql
CREATE DATABASE sber_spark_db;
CREATE ROLE program WITH PASSWORD 'test';
GRANT ALL PRIVILEGES ON DATABASE sber_spark_db TO program;
ALTER ROLE program WITH LOGIN;