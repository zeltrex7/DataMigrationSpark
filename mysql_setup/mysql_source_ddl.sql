CREATE DATABASE IF NOT EXISTS mysql_source;
USE mysql_source;

CREATE TABLE IF NOT EXISTS mysql_source.schema_master(
schema_mstr_key INT AUTO_INCREMENT ,
version INT NOT NULL,
schema_name VARCHAR(250)  NOT NULL,
is_active BOOLEAN NOT NULL,
action VARCHAR(250),
creation_date DATETIME NOT NULL,
PRIMARY KEY(schema_mstr_key , version , schema_name)
);

CREATE TABLE IF NOT EXISTS mysql_source.table_master(
table_mstr_key INT AUTO_INCREMENT ,
version INT NOT NULL,
table_name VARCHAR(250)  NOT NULL,
schema_mstr_key int not null,
is_active BOOLEAN NOT NULL,
action VARCHAR(250),
creation_date DATETIME NOT NULL,
PRIMARY KEY(table_mstr_key , version , table_name)
);

CREATE TABLE IF NOT EXISTS mysql_source.field_master(
field_mstr_key INT AUTO_INCREMENT,
version INT NOT NULL,
field_name VARCHAR(250)  NOT NULL,
data_type_mstr_key INT NOT NULL,
is_active BOOLEAN NOT NULL,
action VARCHAR(250),
creation_date DATETIME NOT NULL,
PRIMARY KEY(field_mstr_key , version , field_name)
);

CREATE TABLE IF NOT EXISTS mysql_source.data_type_master(
data_type_mstr_key INT AUTO_INCREMENT ,
version INT NOT NULL,
data_type VARCHAR(250)  NOT NULL,
is_active BOOLEAN NOT NULL,
action VARCHAR(250),
creation_date DATETIME NOT NULL,
PRIMARY KEY(data_type_mstr_key , version , data_type)
);

