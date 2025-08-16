CREATE USER 'hive'@'localhost' IDENTIFIED BY 'hive_password';
GRANT ALL PRIVILEGES ON hive_metastore.* TO 'hive'@'localhost';
FLUSH PRIVILEGES;