CREATE USER kafka_connect WITH PASSWORD 'kafka_connect';
CREATE DATABASE kafka_connect;
GRANT ALL PRIVILEGES ON DATABASE kafka_connect TO kafka_connect;
CREATE TABLE test_topic (
    id serial,
    name text,
    location text
);
INSERT INTO test_topic (id, name, location) VALUES (1, 'name', 'location')