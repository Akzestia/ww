CREATE KEYSPACE IF NOT EXISTS ww
WITH REPLICATION = {
    'class' : 'SimpleStrategy',
    'replication_factor' : 1
};

USE "ww";

CREATE TABLE IF NOT EXISTS projects(
    name        TEXT PRIMARY KEY,
    env_variables map<TEXT, TEXT>,
);
