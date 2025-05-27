ATTACH './database.db' AS db1;
ATTACH './database2.db' AS db2;
COPY FROM DATABASE db1 TO db2;