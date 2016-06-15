# Neo4j Transfer

Directly transfer nodes and relations between Neo4j servers or backup/restore entire databases to/from files.


## Building

This tool may be built as a stand-alone phar file by using the following commands:

```
git clone https://github.com/liutec/neo4jtransfer.git
cd neo4jtransfer
./bin/build
./neo4jtransfer.phar help
```


## Commands and arguments

The following commands are available:


### The dump command

Dump all nodes and relations from a Neo4j database into a cypher file.


#### Dump command example

The following command will dump all nodes and relations from `neo4j1-prod` into a file. (eg. dump-neo4j1-prod-20160615-233212.cypher)

```
./neo4jtransfer.phar dump --output=default --source-host=neo4j1-prod
```


#### Dump command arguments

| Argument                       | Default value | Description                                                                                                                                                                                                |
|--------------------------------|---------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--source-host`                | `localhost`   | Neo4j source server hostname.                                                                                                                                                                              |
| `--source-port`                | `7474`        | Neo4j source server port.                                                                                                                                                                                  |
| `--source-user`                | `neo4j`       | Neo4j source server username.                                                                                                                                                                              |
| `--source-password`            | `neo4j`       | Neo4j source server password.                                                                                                                                                                              |
| `--output`                     | `STDOUT`      | Cypher output filename. If unspecified, will use STDOUT. Set to `default` to use `dump-[source-host]-[yyyyMMdd]-[hhmmss].cypher`                                                                           |
| `--read-batch-size`            | `300`         | The number of nodes and relations to read at once.                                                                                                                                                         |
| `--node-batch-size`            | `100`         | The number of nodes to write as part of a single cypher query or batch.                                                                                                                                    |
| `--relation-batch-size`        | `150`         | The number of relations to write as part of a single cypher query or batch.                                                                                                                                |
| `--ignore-relation-properties` | `none`        | Comma separated values of properties to be ignored for relations. . (eg. creationDate,modificationDate)                                                                                                    |
| `--preserve-ids`               | `none`        | Comma separated list of label attributes to treat as Node IDs. (eg. Job.ownerId,Action.userId)                                                                                                             |
| `--clean`                      | `true`        | Set to `false` not to clean target database before importing. By default all nodes and relations will be removed.                                                                                          |
| `--transactional`              | `false`       | Set to `true` to wrap all cyphers in a transaction. Non-transactional by default.                                                                                                                          |
| `--import-label`               | `_ilb`        | The name of the label set on imported nodes to create a temporary index with which to quicker identify nodes when transferring relations. This label and the index on it will be removed after the import. |
| `--import-id-key`              | `_iid`        | The name of the key used to hold the node IDs as imported. This attribute and the index on it will be removed after the import.                                                                            |


### The import command

Run cypher queries from a file.


#### Import command example

The following command will look for the latest dump corresponding to the `neo4j1-prod` host (eg. dump-neo4j1-prod-20160615-233212.cypher) and execute the cypher queries within onto the Neo4j database on `localhost`.

```
./neo4jtransfer.phar import --input=last:neo4j1-prod
```

#### Import command arguments

| Argument            | Default value | Description                                                                                                                                                    |
|---------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `--input`           | `STDIN`       | Cypher input filename. If unspecified, will use STDIN. Set to `last:[hostname]` to use `dump-[hostname]-[yyyyMMdd]-[hhmmss].cypher` with the latest timestamp. |
| `--target-host`     | `localhost`   | Neo4j target server hostname.                                                                                                                                  |
| `--target-port`     | `7474`        | Neo4j target server port.                                                                                                                                      |
| `--target-user`     | `neo4j`       | Neo4j target server username.                                                                                                                                  |
| `--target-password` | `neo4j`       | Neo4j target server password.                                                                                                                                  |


### The direct transfer command

Transfer nodes and relations from one Neo4j database into another without an intermediary dump file.


#### Direct transfer command example

The following command will transfer all nodes and relations from `neo4j1-prod` into `app1-dev` within a single transaction.

```
./neo4jtransfer.phar direct --source-host=neo4j1-prod --target-host=app1-dev --transactional
```

#### Direct transfer command arguments

| Argument                       | Default value | Description                                                                                                       |
|--------------------------------|---------------|-------------------------------------------------------------------------------------------------------------------|
| `--source-host`                | `localhost`   | Neo4j source server hostname.                                                                                     |
| `--source-port`                | `7474`        | Neo4j source server port.                                                                                         |
| `--source-user`                | `neo4j`       | Neo4j source server username.                                                                                     |
| `--source-password`            | `neo4j`       | Neo4j source server password.                                                                                     |
| `--target-host`                | `localhost`   | Neo4j target server hostname.                                                                                     |
| `--target-port`                | `7474`        | Neo4j target server port.                                                                                         |
| `--target-user`                | `neo4j`       | Neo4j target server username.                                                                                     |
| `--target-password`            | `neo4j`       | Neo4j target server password.                                                                                     |
| `--read-batch-size`            | `300`         | The number of nodes and relations to read at once.                                                                |
| `--node-batch-size`            | `100`         | The number of nodes to write as part of a single cypher query or batch.                                           |
| `--relation-batch-size`        | `150`         | The number of relations to write as part of a single cypher query or batch.                                       |
| `--ignore-relation-properties` | `none`        | Comma separated values of properties to be ignored for relations. . (eg. creationDate,modificationDate)           |
| `--preserve-ids`               | `none`        | Comma separated list of label attributes to treat as Node IDs. (eg. Job.ownerId,Action.userId)                    |
| `--clean`                      | `true`        | Set to `false` not to clean target database before importing. By default all nodes and relations will be removed. |
| `--transactional`              | `false`       | Set to `true` to wrap all cyphers in a transaction. Non-transactional by default.                                 |


### The transfer command

Transfer nodes and relations from one Neo4j database into another and save all cypher queries to a dump file.


#### Transfer command example

The following command will transfer all nodes and relations from `neo4j1-prod` into `app1-dev` within a single transaction and create a dump file for backup. (eg. dump-neo4j1-prod-20160615-233212.cypher)

```
./neo4jtransfer.phar transfer --output=default --source-host=neo4j1-prod --target-host=app1-dev
```

#### Direct transfer command arguments

| Argument                       | Default value | Description                                                                                                                      |
|--------------------------------|---------------|----------------------------------------------------------------------------------------------------------------------------------|
| `--source-host`                | `localhost`   | Neo4j source server hostname.                                                                                                    |
| `--source-port`                | `7474`        | Neo4j source server port.                                                                                                        |
| `--source-user`                | `neo4j`       | Neo4j source server username.                                                                                                    |
| `--source-password`            | `neo4j`       | Neo4j source server password.                                                                                                    |
| `--output`                     | `STDOUT`      | Cypher output filename. If unspecified, will use STDOUT. Set to `default` to use `dump-[source-host]-[yyyyMMdd]-[hhmmss].cypher` |
| `--target-host`                | `localhost`   | Neo4j target server hostname.                                                                                                    |
| `--target-port`                | `7474`        | Neo4j target server port.                                                                                                        |
| `--target-user`                | `neo4j`       | Neo4j target server username.                                                                                                    |
| `--target-password`            | `neo4j`       | Neo4j target server password.                                                                                                    |
| `--read-batch-size`            | `300`         | The number of nodes and relations to read at once.                                                                               |
| `--node-batch-size`            | `100`         | The number of nodes to write as part of a single cypher query or batch.                                                          |
| `--relation-batch-size`        | `150`         | The number of relations to write as part of a single cypher query or batch.                                                      |
| `--ignore-relation-properties` | `none`        | Comma separated values of properties to be ignored for relations. . (eg. creationDate,modificationDate)                          |
| `--preserve-ids`               | `none`        | Comma separated list of label attributes to treat as Node IDs. (eg. Job.ownerId,Action.userId)                                   |
| `--clean`                      | `true`        | Set to `false` not to clean target database before importing. By default all nodes and relations will be removed.                |
| `--transactional`              | `false`       | Set to `true` to wrap all cyphers in a transaction. Non-transactional by default.                                                |
