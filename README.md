# pytella
A Jython wrapper to run ETL scripts with scriptella.

## Planned Implementations
  - Support:
    - ~~Oracle~~
    - SQL Server
    - ~~PostgreSQL~~
    - ~~MySQL~~
    - MongoDB
    - SQLite
    - ~~CSV~~
  - Automatically create table in target (if not exists) -- tricky for cross-DB systems. Need to translate data types.
  - Add caching mechanism (to not need to obtain table source/target structure each time). Use MD5 sum or some Sha comparison to determine if the workflow config text has changed.
  - ~~ More complex mappings through YML definitions ~~
    - ~~custom field mapping~~
    - ~~custom expression support~~

## Requirements
- Java (7 or later) needs to be installed.
- Create `settings.yml` from `setting.template.yml`, add databases/credentials
- Create an YML etl file (such as `etl_test1.yml`), containing your workflows / mappings. See `etl_.template.yml` as an example. The YAML structure needs to be followed.

## Oracle Driver Setup

## Execution

```bash
git clone https://github.com/flarco/pytella.git
cd pytella

# Examples:
java -cp "lib/*" org.python.util.jython pytella.py -workflow etl_test1.yml -showDetails
java -cp "lib/*" org.python.util.jython pytella.py -sConn DB1 -tConn DB2 -sTable SC.TABLE1 -tTable SS.TABLE2 -truncate -showDetails
java -cp "lib/*" org.python.util.jython pytella.py -mapping "DB1.SC.TABLE1 > DB2.SS.TABLE2" -truncate -showDetails -batchSize 50000

```