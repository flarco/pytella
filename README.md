# pytella
A Jython wrapper to run ETL scripts with scriptella.

## Planned Implementations
  - Support:
    - Oracle
    - ~~SQL Server~~
    - ~~PostgreSQL~~
    - ~~MySQL~~
    - MongoDB
    - SQLite
    - CSV
  - Automatically create table in target (if not exists)
  - More complex mappings through YML definitions

## Requirements
- Java (7 or later) needs to be installed.
- create `settings.yml` from `setting.template.yml`, add databases/credentials

## Execution

```bash
git clone https://github.com/flarco/pytella.git
cd pytella

java -cp "lib/*" org.python.util.jython pythella.py -sConn DBPRD -tConn DBDEV -sTable SC.TABLE1 -tTable SS.TABLE2 -truncate -showDetails
java -cp "lib/*" org.python.util.jython pythella.py -workflow etl_test1.yml -showDetails -batchSize 50000
java -cp "lib/*" org.python.util.jython pythella.py -mapping "DBPRD.SC.TABLE1:STGDEV.DBDEV.SS.TABLE2" -truncate -showDetails -batchSize 50000

```