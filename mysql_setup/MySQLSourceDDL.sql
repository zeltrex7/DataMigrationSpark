CREATE DATABASE IF NOT EXISTS mysql_source;
USE mysql_source;

DROP TABLE IF  EXISTS mysql_source.schema_master;
CREATE TABLE IF NOT EXISTS mysql_source.schema_master(
schema_mstr_key INT AUTO_INCREMENT ,
version INT NOT NULL,
db_type_mstr_key INT NOT NULL,
schema_name VARCHAR(250)  NOT NULL,
complete_refresh BOOLEAN DEFAULT FALSE NOT NULL,
is_active BOOLEAN NOT NULL,
action VARCHAR(250),
creation_date DATETIME NOT NULL,
PRIMARY KEY(schema_mstr_key , version , db_type_mstr_key ,schema_name)
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

DROP TABLE IF exists mysql_source.field_master;
CREATE TABLE IF NOT EXISTS mysql_source.field_master(
field_mstr_key INT AUTO_INCREMENT,
table_mstr_key INT NOT NULL,
version INT NOT NULL,
field_name VARCHAR(250)  NOT NULL,
data_type_mstr_key INT NOT NULL,
max_length INT,
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


DROP TABLE IF EXISTS mysql_source.db_type_master;
CREATE TABLE IF NOT EXISTS mysql_source.db_type_master(
db_type_mstr_key INT AUTO_INCREMENT,
version INT NOT NULL,
db_name VARCHAR(250) NOT NULL,
is_active BOOLEAN NOT NULL,
action VARCHAR(250),
creation_date DATETIME NOT NULL,
PRIMARY KEY(db_type_mstr_key , version , db_name)
);

DROP TABLE IF EXISTS mysql_source.data_type_db_casting_master;
CREATE TABLE IF NOT EXISTS mysql_source.data_type_db_casting_master(
data_type_db_casting_mstr_key INT AUTO_INCREMENT ,
version INT NOT NULL,
source_db_mstr_key VARCHAR(250) NOT NULL,
source_data_type_mstr_key VARCHAR(250)  NOT NULL,
target_db_mstr_key VARCHAR(250) NOT NULL,
target_data_type_mstr_key VARCHAR(250)  NOT NULL,
is_active BOOLEAN NOT NULL,
action VARCHAR(250),
creation_date DATETIME NOT NULL,
PRIMARY KEY(data_type_db_casting_mstr_key , version , source_data_type_mstr_key, target_data_type_mstr_key)
);


DROP TABLE IF EXISTS mysql_source.exclusion_rule_type_master;
CREATE TABLE IF NOT EXISTS mysql_source.exclusion_rule_type_master(
exclusion_rule_type_mstr_key INT AUTO_INCREMENT ,
version INT NOT NULL,
exclusion_rule_type VARCHAR(250) NOT NULL,
is_active BOOLEAN NOT NULL,
action VARCHAR(250),
creation_date DATETIME NOT NULL,
PRIMARY KEY(exclusion_rule_type_mstr_key , version , exclusion_rule_type)
);

DROP TABLE IF EXISTS mysql_source.exclusion_rule_master;
CREATE TABLE IF NOT EXISTS mysql_source.exclusion_rule_master(
exclusion_rule_mstr_key INT AUTO_INCREMENT ,
version INT NOT NULL,
exclusion_rule_type_mstr_key INT NOT NULL,
excluded_entity_mstr_key INT NOT NULL,
is_active BOOLEAN NOT NULL,
action VARCHAR(250),
creation_date DATETIME NOT NULL,
PRIMARY KEY(exclusion_rule_mstr_key , version , exclusion_rule_type_mstr_key, excluded_entity_mstr_key)
);


