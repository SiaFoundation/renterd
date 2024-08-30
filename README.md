# [![Sia](https://sia.tech/assets/banners/sia-banner-expanded-renterd.png)](http://sia.tech)
[![GoDoc](https://godoc.org/go.sia.tech/renterd?status.svg)](https://godoc.org/go.sia.tech/renterd)


`renterd` is an advanced Sia renter engineered by the Sia Foundation. Designed
to cater to both casual users seeking straightforward data storage and
developers requiring a robust API for building apps on Sia

## Overview

`renterd` is the successor to `siad`, offering feature parity while extending
its capabilities with new features like an enhanced web UI and an autopilot.
That said, `renterd` does not support backwards compatibility with siad
metadata. Consequently, files uploaded via siad cannot currently be migrated to
renterd. Our immediate focus is on refining `renterd` to enhance its stability,
scalability, and performance, ensuring it serves as a robust foundation for new
Sia applications. Useful links:

- [API documentation](https://api.sia.tech/renterd)
- [Project Roadmap](https://github.com/orgs/SiaFoundation/projects/5)
- [Setup Guide](https://docs.sia.tech/renting/setting-up-renterd)

## Database

`renterd` requires a database to store its operational data. We support both
SQLite and MySQL, with SQLite set as the default due to its ease of setup.
SQLite is ideal for testing and development purposes, whereas MySQL is
recommended for production environments.

## Configuration

`renterd` can be configured in various ways, through the use of a yaml file, CLI
flags or environment variables. Settings that are configured multiple times will
be evaluated in this order. In the CLI, use the `help` command to see an
overview of all settings configurable through the CLI.

| **Name**                             | **Description**                                      | **Default Value**                 | **CLI Flag**                     | **Environment Variable**                       | **YAML Path**                           |
|--------------------------------------|------------------------------------------------------|-----------------------------------|----------------------------------|------------------------------------------------|----------------------------------------|
| `HTTP.Address`                       | Address for serving the API                          | `:9980`                          | `--http`                         | -                                              | `http.address`                     |
| `HTTP.Password`                      | Password for the HTTP server                         | -                                 | -                                | `RENTERD_API_PASSWORD`                         | `http.password`                     |
| `Directory`                          | Directory for storing node state                     | `.`                               | `--dir`                          | -                                              | `directory`                        |
| `Seed`                               | Seed for the node                                    | -                                 | -                                | `RENTERD_SEED`                                 | `seed`                              |
| `AutoOpenWebUI`                      | Automatically open the web UI on startup             | `true`                            | `--openui`                       | -                                              | `autoOpenWebUI`                    |
| `Network`                            | Network to run on (mainnet/zen/anagami)  | `mainnet`                          | `--network`                       | `RENTERD_NETWORK`                             | `network`                    |
| `ShutdownTimeout`                    | Timeout for node shutdown                            | `5m`                              | `--node.shutdownTimeout`         | -                                              | `shutdownTimeout`                  |
| `Log.Level`                          | Global logger level (debug\|info\|warn\|error). Defaults to 'info' | `info`                            | `--log.level`               | `RENTERD_LOG_LEVEL`                          | `log.level`                         |
| `Log.File.Enabled`                   | Enables logging to disk. Defaults to 'true'          | `true`                            | `--log.file.enabled`              | `RENTERD_LOG_FILE_ENABLED`                   | `log.file.enabled`                  |
| `Log.File.Format`                    | Format of log file (json\|human). Defaults to 'json'  | `json`                            | `--log.file.format`               | `RENTERD_LOG_FILE_FORMAT`                    | `log.file.format`                   |
| `Log.File.Path`                      | Path of log file. Defaults to 'renterd.log' within the renterd directory | `renterd.log`                     | `--log.file.path`       | `RENTERD_LOG_FILE_PATH`                      | `log.file.path`                     |
| `Log.StdOut.Enabled`                 | Enables logging to stdout. Defaults to 'true'        | `true`                            | `--log.stdout.enabled`            | `RENTERD_LOG_STDOUT_ENABLED`                 | `log.stdout.enabled`                |
| `Log.StdOut.Format`                  | Format of log output (json\|human). Defaults to 'human' | `human`                            | `--log.stdout.format`           | `RENTERD_LOG_STDOUT_FORMAT`                  | `log.stdout.format`                 |
| `Log.StdOut.EnableANSI`              | Enables ANSI color codes in log output. Defaults to 'true' on non-Windows systems | `true` (`false` on Windows)       | `--log.stdout.enableANSI` | `RENTERD_LOG_STDOUT_ENABLE_ANSI`             | `log.stdout.enableANSI`             |
| `Log.Database.Enabled`               | Enable logging database queries. Defaults to 'true'  | `true`                            | `--log.database.enabled`          | `RENTERD_LOG_DATABASE_ENABLED`               | `log.database.enabled`              |
| `Log.Database.Level`                 | Logger level for database queries (info\|warn\|error). Defaults to 'warn' | `warn`                            | `--log.database.level`          | `RENTERD_LOG_DATABASE_LEVEL`, `RENTERD_LOG_LEVEL` | `log.database.level`           |
| `Log.Database.IgnoreRecordNotFoundError` | Enable ignoring 'not found' errors resulting from database queries. Defaults to 'true' | `true`                            | `--log.database.ignoreRecordNotFoundError` | `RENTERD_LOG_DATABASE_IGNORE_RECORD_NOT_FOUND_ERROR` | `log.database.ignoreRecordNotFoundError` |
| `Log.Database.SlowThreshold`         | Threshold for slow queries in logger. Defaults to 100ms | `100ms`                          | `--log.database.slowThreshold`  | `RENTERD_LOG_DATABASE_SLOW_THRESHOLD`         | `log.database.slowThreshold`        |
| `Log.Database.Level (DEPRECATED)`    | Logger level                                        | `warn`                            | `--db.logger.logLevel`           | `RENTERD_DB_LOGGER_LOG_LEVEL`                  | `log.database.level`               |
| `Log.Database.IgnoreRecordNotFoundError (DEPRECATED)` | Ignores 'not found' errors in logger    | `true`                            | `--db.logger.ignoreNotFoundError`| `RENTERD_DB_LOGGER_IGNORE_NOT_FOUND_ERROR`     | `log.ignoreRecordNotFoundError` |
| `Log.Database.SlowThreshold (DEPRECATED)`    | Threshold for slow queries in logger    | `100ms`                          | `--db.logger.slowThreshold`      | `RENTERD_DB_LOGGER_SLOW_THRESHOLD`             | `log.slowThreshold`       |
| `Log.Path (DEPRECATED)`              | Path to directory for logs                          | -                                 | `--log-path`                     | `RENTERD_LOG_PATH`                             | `log.path`                         |
| `Database.MySQL.URI`                 | Database URI for the bus                             | -                                 | `--db.uri`                      | `RENTERD_DB_URI`                              | `database.mysql.uri`                |
| `Database.MySQL.User`                | Database username for the bus                        | `renterd`                         | `--db.user`                     | `RENTERD_DB_USER`                             | `database.mysql.user`               |
| `Database.MySQL.Password`            | Database password for the bus                        | -                                 | -                               | `RENTERD_DB_PASSWORD`                         | `database.mysql.password`           |
| `Database.MySQL.Database`            | Database name for the bus                            | `renterd`                         | `--db.name`                     | `RENTERD_DB_NAME`                             | `database.mysql.database`           |
| `Database.MySQL.MetricsDatabase`     | Database for metrics                                 | `renterd_metrics`                 | `--db.metricsName`              | `RENTERD_DB_METRICS_NAME`                     | `database.mysql.metricsDatabase`    |
| `Database.SQLite.Database`           | SQLite database name                                 | -                                 | -                               | -                                              | `database.sqlite.database`          |
| `Database.SQLite.MetricsDatabase`    | SQLite metrics database name                         | -                                 | -                               | -                                              | `database.sqlite.metricsDatabase`   |
| `Bus.AnnouncementMaxAgeHours`        | Max age for announcements                            | `8760h` (1 year)                  | `--bus.announcementMaxAgeHours` | -                                              | `bus.announcementMaxAgeHours`       |
| `Bus.Bootstrap`                      | Bootstraps gateway and consensus modules             | `true`                            | `--bus.bootstrap`               | -                                              | `bus.bootstrap`                     |
| `Bus.GatewayAddr`                    | Address for Sia peer connections                     | `:9981`                          | `--bus.gatewayAddr`             | `RENTERD_BUS_GATEWAY_ADDR`                     | `bus.gatewayAddr`                   |
| `Bus.RemoteAddr`                     | Remote address for the bus                           | -                                 | -                               | `RENTERD_BUS_REMOTE_ADDR`                      | `bus.remoteAddr`                    |
| `Bus.RemotePassword`                 | Remote password for the bus                          | -                                 | -                               | `RENTERD_BUS_API_PASSWORD`                     | `bus.remotePassword`                |
| `Bus.PersistInterval`                | Interval for persisting consensus updates            | `1m`                              | `--bus.persistInterval`         | -                                              | `bus.persistInterval`               |
| `Bus.UsedUTXOExpiry`                 | Expiry for used UTXOs in transactions                | `24h`                             | `--bus.usedUTXOExpiry`          | -                                              | `bus.usedUtxoExpiry`                |
| `Bus.SlabBufferCompletionThreshold`  | Threshold for slab buffer upload                     | `4096`                            | `--bus.slabBufferCompletionThreshold` | `RENTERD_BUS_SLAB_BUFFER_COMPLETION_THRESHOLD` | `bus.slabBufferCompletionThreshold` |
| `Worker.AllowPrivateIPs`             | Allows hosts with private IPs                        | -                                 | `--worker.allowPrivateIPs`       | -                                              | `worker.allowPrivateIPs`            |
| `Worker.BusFlushInterval`            | Interval for flushing data to bus                    | `5s`                              | `--worker.busFlushInterval`      | -                                              | `worker.busFlushInterval`           |
| `Worker.ContractLockTimeout`         | Timeout for locking contracts                        | `30s`                             | -                               | -                                              | `worker.contractLockTimeout`        |
| `Worker.DownloadMaxOverdrive`        | Max overdrive workers for downloads                  | `5`                               | `--worker.downloadMaxOverdrive`  | -                                              | `worker.downloadMaxOverdrive`       |
| `Worker.DownloadMaxMemory`           | Max memory for downloads                             | `1GiB`                            | `--worker.downloadMaxMemory`     | `RENTERD_WORKER_DOWNLOAD_MAX_MEMORY`           | `worker.downloadMaxMemory`          |
| `Worker.ID`                          | Unique ID for worker                                 | `worker`                          | `--worker.id`                    | `RENTERD_WORKER_ID`                            | `worker.id`                         |
| `Worker.DownloadOverdriveTimeout`    | Timeout for overdriving slab downloads               | `3s`                              | `--worker.downloadOverdriveTimeout` | -                                            | `worker.downloadOverdriveTimeout`   |
| `Worker.UploadMaxMemory`             | Max amount of RAM the worker allocates for slabs when uploading | `1GiB`                 | `--worker.uploadMaxMemory`      | `RENTERD_WORKER_UPLOAD_MAX_MEMORY`             | `worker.uploadMaxMemory`            |
| `Worker.UploadMaxOverdrive`          | Max overdrive workers for uploads                    | `5`                               | `--worker.uploadMaxOverdrive`    | -                                              | `worker.uploadMaxOverdrive`         |
| `Worker.UploadOverdriveTimeout`      | Timeout for overdriving slab uploads                 | `3s`                              | `--worker.uploadOverdriveTimeout` | -                                              | `worker.uploadOverdriveTimeout`     |
| `Worker.Enabled`                     | Enables/disables worker                              | `true`                            | `--worker.enabled`               | `RENTERD_WORKER_ENABLED`                       | `worker.enabled`                    |
| `Worker.AllowUnauthenticatedDownloads` | Allows unauthenticated downloads                    | -                                 | `--worker.unauthenticatedDownloads` | `RENTERD_WORKER_UNAUTHENTICATED_DOWNLOADS` | `worker.allowUnauthenticatedDownloads` |
| `Worker.ExternalAddress`              | Address of the worker on the network, only necessary when the bus is remote | -                                 | -                                | `RENTERD_WORKER_EXTERNAL_ADDR`                     | `worker.externalAddress`                   |
| `Worker.RemoteAddrs`                 | List of remote worker addresses (semicolon delimited) | -                                | -                                | `RENTERD_WORKER_REMOTE_ADDRS`                     | `worker.remotes`                    |
| `Worker.RemotePassword`               | API password for the remote workers                 | -                                | -                                | `RENTERD_WORKER_API_PASSWORD`                     | `worker.remotes`              |
| `Autopilot.Enabled`					| Enables/disables autopilot							| `true`							| `--autopilot.enabled`			| `RENTERD_AUTOPILOT_ENABLED`						| `autopilot.enabled`					|
| `Autopilot.AccountsRefillInterval`   | Interval for refilling workers' account balances     | `24h`                             | `--autopilot.accountRefillInterval` | -                                              | `autopilot.accountsRefillInterval`  |
| `Autopilot.Heartbeat`                | Interval for autopilot loop execution                | `30m`                             | `--autopilot.heartbeat`            | -                                              | `autopilot.heartbeat`               |
| `Autopilot.MigrationHealthCutoff`    | Threshold for migrating slabs based on health        | `0.75`                            | `--autopilot.migrationHealthCutoff` | -                                              | `autopilot.migrationHealthCutoff`   |
| `Autopilot.RevisionBroadcastInterval`| Interval for broadcasting contract revisions         | `168h` (7 days)                   | `--autopilot.revisionBroadcastInterval` | `RENTERD_AUTOPILOT_REVISION_BROADCAST_INTERVAL` | `autopilot.revisionBroadcastInterval` |
| `Autopilot.ScannerBatchSize`         | Batch size for host scanning                         | `1000`                            | `--autopilot.scannerBatchSize`      | -                                              | `autopilot.scannerBatchSize`        |
| `Autopilot.ScannerInterval`          | Interval for scanning hosts                          | `24h`                             | `--autopilot.scannerInterval`       | -                                              | `autopilot.scannerInterval`         |
| `Autopilot.ScannerNumThreads`        | Number of threads for scanning hosts                 | `100`                             | -                                | -                                              | `autopilot.scannerNumThreads`       |
| `Autopilot.MigratorParallelSlabsPerWorker` | Parallel slab migrations per worker                    | `1`                               | `--autopilot.migratorParallelSlabsPerWorker` | `RENTERD_MIGRATOR_PARALLEL_SLABS_PER_WORKER` | `autopilot.migratorParallelSlabsPerWorker` |
| `S3.Address`                         | Address for serving S3 API                           | `:9982`                          | `--s3.address`                     | `RENTERD_S3_ADDRESS`                           | `s3.address`                        |
| `S3.DisableAuth`                     | Disables authentication for S3 API                   | `false`                           | `--s3.disableAuth`                 | `RENTERD_S3_DISABLE_AUTH`                      | `s3.disableAuth`                    |
| `S3.Enabled`                         | Enables/disables S3 API                              | `true`                            | `--s3.enabled`                     | `RENTERD_S3_ENABLED`                           | `s3.enabled`                        |
| `S3.HostBucketBases`       | Enables bucket rewriting in the router for the provided bases  | -                                 | `--s3.hostBucketBases`           | `RENTERD_S3_HOST_BUCKET_BASES`               | `s3.hostBucketBases`              |
| `S3.HostBucketEnabled`               | Enables bucket rewriting in the router               | -                                 | `--s3.hostBucketEnabled`           | `RENTERD_S3_HOST_BUCKET_ENABLED`               | `s3.hostBucketEnabled`              |
| `S3.KeypairsV4 (DEPRECATED)`                      | V4 keypairs for S3                                   | -                                 | -                                  | -            | `s3.keypairsV4`                     |

### Single-Node Setup

A single-node setup involves running all components (bus, worker, and autopilot)
on the same machine. This is ideal for testing, development, or small-scale
deployments. This setup is the default when running `renterd` without any flags.

### Cluster Setup

In a cluster setup, the bus, worker, and autopilot run on separate nodes. This
setup is ideal for large-scale deployments where you want to horizontally scale
your renter. The worker nodes can be spread across multiple machines, and the
autopilot can be run on a separate machine.

#### Bus Node Configuration

The bus is the only node that exposes the UI. To run the bus separately, the
autopilot and worker have to be disabled using the `--autopilot.enabled` and
`--worker.enabled` flags. The only other requirement to run a bus is the (walet)
seed.

#### Worker Node Configuration

To configure the worker as a standalone node, the autopilot has to be disabled
using the `--autopilot.enabled` flag, and the bus has to be disabled. There's no
flag to explicitly disable the `bus`, it's implied by configuring a remote
address for the bus using the `--bus.remoteAddr` and `--bus.remotePassword`
flags. When the bus is remote, the worker has to be configured with an external
address of the form `http://<worker-ip>:<port>`, on localhost however this can be
the same as the worker's HTTP address. The worker needs to know its location on
the network because it relies on some webhooks it needs to register with the
bus, which in turn needs to know how to reach the worker when certain events
occur. Therefor it is important to start the worker after the bus is reachable.

#### Autopilot Node Configuration

To run the autopilot separately, the worker has to be disabled using the
`--worker.enabled` flag. Similar to the worker, the autopilot has to be
configured with a remote bus for the node not to start a bus itself. Alongside
with knowing where the bus is located, the autopilot also has to be aware of the
workers. These remote workers can be configured through yaml under the option
`worker.remotes`, or through environment variables
(`RENTERD_WORKER_REMOTE_ADDRS` and `RENTERD_WORKER_API_PASSWORD`).

#### Example docker-compose with minimal configuration

```yaml
version: '3.9'

services:
  bus:
    image: ghcr.io/siafoundation/renterd:master
    container_name: renterd_bus
    environment:
      - RENTERD_SEED=<enter seed here>
      - RENTERD_API_PASSWORD=bus-pass
    ports:
      - "9980:9980"
      - "9981:9981"

  worker-1:
    image: ghcr.io/siafoundation/renterd:master
    container_name: renterd_worker-1
    environment:
      - RENTERD_AUTOPILOT_ENABLED=false
      - RENTERD_SEED=<enter seed here>
      - RENTERD_API_PASSWORD=worker-pass
      - RENTERD_BUS_API_PASSWORD=bus-pass
      - RENTERD_BUS_REMOTE_ADDR=http://bus:9980/api/bus
      - RENTERD_WORKER_EXTERNAL_ADDR=http://worker-1:9980/api/worker
    ports:
      - "9982:9980"
      - "8082:8080"
    depends_on:
      - bus

  worker-2:
    image: ghcr.io/siafoundation/renterd:master
    container_name: renterd_worker-2
    environment:
      - RENTERD_SEED=<enter seed here>
      - RENTERD_API_PASSWORD=worker-pass
      - RENTERD_BUS_API_PASSWORD=bus-pass
      - RENTERD_BUS_REMOTE_ADDR=http://bus:9980/api/bus
      - RENTERD_WORKER_EXTERNAL_ADDR=http://worker-2:9980/api/worker
    ports:
      - "9983:9980"
      - "8083:8080"
    depends_on:
      - bus

  autopilot:
    image: ghcr.io/siafoundation/renterd:master
    container_name: renterd_autopilot
    environment:
      - RENTERD_API_PASSWORD=autopilot-pass
      - RENTERD_BUS_API_PASSWORD=bus-pass
      - RENTERD_BUS_REMOTE_ADDR=http://bus:9980/api/bus
      - RENTERD_WORKER_API_PASSWORD=<worker-password>
      - RENTERD_WORKER_REMOTE_ADDRS=http://worker-1:9980/api/worker;http://worker-2:9980/api/worker
    ports:
      - "9984:9980"
    depends_on:
      - bus
      - worker-1
      - worker-2
```

## Tweaking Performance

Depending on hardware specs, you can change the [configuration](#configuration)
to better utilize it and gain more performance out of `renterd`. This section
highlights some of the more obvious tweaks one can apply.

### Increase/Decrease memory

By default, `renterd` uses reasonable limits for RAM consumed by uploads and
downloads. Especially when downloading or uploading single large files, more RAM
can make a difference since it allows for processing the download/upload in
parallel. To change the max RAM `renterd` is going to use update the
`Worker.DownloadMaxMemory` and `Worker.UploadMaxMemory` settings.

### Overdrive

Both uploads and downloads have a setting we call "overdrive". Since `renterd`
operates in a trustless environment, we can't rely on all of our hosts being
reliable and of high quality. So when uploading `n` shards of some data to the
network (or downloading from it), the process is bottlenecked by the slowest
host. That is where the overdrive comes in.

`Worker.UploadMaxOverdrive` and `Worker.DownloadMaxOverdrive` can be used to
configure how many additional hosts to the number we need to upload/download we
use to reduce the chance of getting hung up on a slow one. The default is `3`
which means up to 3 hosts can get stuck with the upload/download remaining
mostly unaffected. `Worker.UploadOverdriveTimeout` and
`Worker.DownloadOverdriveTimeout` specify the time that needs to pass before we
launch the overdrive uploads/downloads.

Two conditions need to be met before the overdrive launches:
1. When uploading/downloading to/from `n` hosts (without overdrive), `n - overdriveHosts` pieces need to finish.
2. Once condition 1. is met, the configured overdrive timeout needs to pass

What this means is that there is a tradeoff between using/paying for more
bandwidth and the ability to compensate for slow/stuck hosts. If you handpick
hosts you trust to be reliable, you can set the max overdrive to 0 for more max
efficiency while you can also increase the overdrive to 30 hosts after 100ms for
faster uploads at the cost of uploading more data than necessary and overpaying.
Regardless, we recommend that you perform your own benchmarking to see what
works best for your set of hosts, budget and use-case.


## Backups

This section provides a step-by-step guide covering the procedures for creating
and restoring backups for `renterd`. Regularly backing up your renter's metadata
and verifying its restorability are essential practices to ensure your data's
integrity and availability in the event of a disaster scenario. Ensure backups
are stored on a different machine than the one running `renterd` to avoid data
loss in case of hardware failure.

---
**IMPORTANT NOTE**

It is important to note that having a backup is not enough to ensure the data
can be recovered from the network. The renter has to be online enough of the
time to ensure data gets migrated away from hosts that went offline.

---

### Creating a backup

#### Step 1: shut down renter

It's strongly recommended to shut down the renter before creating a backup to
ensure data consistency. This precaution addresses the "upload packing" feature
(enabled by default), which relies on on-disk buffers. To capture a consistent
state between the database and on-disk data, shut down `renterd` first. Even if
the feature is not enabled, it is best to shut down the renter before taking a
backup to be on the safe side and ensure consistency.

#### Step 2: backing up the database

`renterd` uses two SQL databases: a main database for contracts, host metadata
and object metadata essential for data retrieval, and a metrics database for UI
features. Both are critical but prioritize the main database for file recovery.

Depending on how the renter is configured, the databases are either SQLite
(default) or MySQL databases. By default, the SQLite databases are called
`db.sqlite` and `metrics.sqlite` and are located in a folder called `db`, right
in the renter's root directory.

**SQLite**

Use the `.backup` command to create a backup of the SQLite databases.

```bash
sqlite3 db.sqlite ".backup 'db.bkp'"
sqlite3 metrics.sqlite ".backup 'metrics.bkp'"
```

There should only be two files in the `db` folder, if you encounter write-ahead
log files or index files (usually named `-wal` or `-shm`) it indicates the
renter was not shut down gracefully. In that case it's best to restart the
renter and shut it down again.

**MySQL**

Use the `mysqldump` command to create a backup of the MySQL databases. It's a
utility provided by MySQL to backup or transfer a MySQL database and it's
usually installed alongside the MySQL client tools. Replace placeholders with
actual user and password.

```bash
mysqldump -u [RENTERD_DB_USER] --password=[RENTERD_DB_PASSWORD] renterd > renterd_bkp.sql
mysqldump -u [RENTERD_DB_USER] --password=[RENTERD_DB_PASSWORD] renterd_metrics > renterd_metrics_bkp.sql
```

#### Step 3: backing up partial slabs

If "upload packing" is enabled, back up the `partial_slabs` folder located in
the renter's root directory. These files contain data that has not been uploaded
to the network yet, losing these files means an immediate loss of your data.

```bash
tar -cvf partial_slabs.tar partial_slabs/
```

### Restoring from a backup

If the goal is to install `renterd` from a backup on a new machine, the easiest
way is to do a fresh `renterd` install and then overwrite the empty database
with the backup. Use the same `RENTERD_SEED` as the original installation.

#### Step 1: shutdown renter

Same as before we advise to shut down the renter before reinstating a backup to
ensure consistency. It's a good idea to backup the database right before trying
to restore a backup to be safe and have a way out in case overwriting the
database renders it corrupt somehow.

#### Step 2: restore the database backup

**SQLite**

For SQLite we can reinstate the database by replacing both `.sqlite` files with
our backups. Make sure to rename them to their original filename `db.sqlite` and
`metrics.sqlite`. These filenames are configurable, so make sure you match the
configured values.

**MySQL**

Depending on when the backup was taken its schema might be out of date. To
maximize the chance the schema migrations go smoothly, it's advisable to
recreate the databases before importing the backup. Take a backup before doing
this.

```bash
# log in to MySQL shell
mysql -u [RENTERD_DB_USER] -p

# recreate renterd database
DROP DATABASE IF EXISTS renterd;
CREATE DATABASE renterd;

# recreate renterd_metrics database
DROP DATABASE IF EXISTS renterd_metrics;
CREATE DATABASE renterd_metrics;
```

The backups can then be imported using the following commands:

```
cat renterd_bkp.sql | mysql -u [RENTERD_DB_USER] --password=[RENTERD_DB_PASSWORD] renterd
cat renterd_metrics_bkp.sql | mysql -u [RENTERD_DB_USER] --password=[RENTERD_DB_PASSWORD] renterd_metrics
```

#### Step 3: restore the partial slabs

If applicable, remove the contents of the `partial_slabs` directory and replace it with your backup.

```bash
rm -rf partial_slabs
tar -xvf partial_slabs.tar
```

#### Step 4: start the renter

After starting the renter it is possible it has to run through migrations to its
database schema. Depending on when the backup was taken, this might take some
time. If we restored the backup on a fresh `renterd` install, it will take some
time for consensus to sync.

## Docker Support

`renterd` includes a `Dockerfile` which can be used for building and running
renterd within a docker container. For the time being it is provided as-is
without any compatibility guarantees as it will change over time and be extended
with more configuration options. The image can also be pulled from
ghcr.io/siafoundation/renterd.

### Docker Compose

```docker
version: "3.9"
services:
  renterd:
    image: ghcr.io/siafoundation/renterd:master
    environment:
      - RENTERD_SEED=put your seed here
      - RENTERD_API_PASSWORD=test
    ports:
      - 9980:9980
      - 9981:9981
      - 8080:8080
    volumes:
      - ./data:/data
    restart: unless-stopped
    stop_grace_period: 5m
```

### Build Image

From within the root of the repo run the following command to build an image of
`renterd` tagged `renterd`.

```sh
docker build -t renterd:master -f ./docker/Dockerfile .
```

### Run Container

Run `renterd` in the background as a container named `renterd` that exposes its
API to the host system and the gateway to the world.


#### Mainnet

```bash
docker run -d --name renterd -e RENTERD_API_PASSWORD="<PASSWORD>" -e RENTERD_SEED="<SEED>" -p 127.0.0.1:9980:9980/tcp -p :9981:9981/tcp ghcr.io/siafoundation/renterd:master
```

#### Testnet

To run `renterd` on testnet use the `RENTERD_NETWORK` environment variable.

```bash
docker run -d --name renterd -e RENTERD_API_PASSWORD="<PASSWORD>" -e RENTERD_NETWORK="<network>" -e RENTERD_SEED="<SEED>" -p 127.0.0.1:9980:9980/tcp -p :9981:9981/tcp ghcr.io/siafoundation/renterd:master
```

Currently available values for `<network>` are:
- `zen`
- `anagami`

## Architecture

`renterd` distinguishes itself from `siad` through a unique architecture
comprised of three main components: the autopilot, the bus, and one or more
workers.

Instead of adopting another Electron app bundle, `renterd` incorporates an
embedded web UI. This approach caters to developers and power users who prefer a
streamlined experience. For those who do not require the UI, `renterd` can be
compiled without it, which reduces software bloat and simplifies the build
process.

### Autopilot

The autopilot in `renterd` automates high-level functions like host scanning,
ranking, and the management of contracts and data. This ensures efficient host
selection for data storage. While it offers convenience for most users, those
desiring full control can disable the autopilot via a CLI flag. Remarkably, the
autopilot is designed over the public `renterd` HTTP API, allowing for
straightforward customization and language porting.

### Bus

Serving as the central nervous system of `renterd`, the bus handles data
persistence and connections with Sia's peer-to-peer network. While `renterd`
defaults to an SQLite database, users have the option to configure it for MySQL
use. SQLite is the default choice due to its seamless initial experience, but
switching to MySQL is recommended for enhanced performance.

### Worker(s)

Workers are the direct interface for users, managing tasks such as file
uploads/downloads and contract handling. They depend on the bus for consistent
data persistence.

## Usage

The Web UI streamlines the initial setup and configuration for newcomers.
However, if manual configuration is necessary, the subsequent sections outline a
step-by-step guide to achieving a functional `renterd` instance.

### Wallet

Make sure the wallet is funded, it's a good rule of thumb to have at least twice
the allowance in the wallet. Fetch the wallet's address and transfer some money.
Verify the wallet's balance is not zero using the following endpoint:

- `GET /api/bus/wallet`

The autopilot will automatically redistribute the wallet funds over a certain
number of outputs that make sense with regards to the autopilot's configuration.
Contract formation and renewals work best when the autopilot has a good amount
of outputs at its disposal. It's definitely a good idea to verify whether this
is the case because if not it means that it's likely the autopilot is
misconfigured, in which case the logs should be of help.

- `GET /api/bus/wallet/outputs`

### Consensus

In order for the contracts to get formed, your node has to be synced with the
blockchain. If you are not bootstrapping your node this can take a while. Verify
your node's consensus state using the following endpoint:

- `GET /api/bus/consensus/state`

### Config

The configuration can be updated through the UI or by using the following
endpoints:

- `GET /api/autopilot/config`
- `PUT /api/autopilot/config`

The autopilot will not perform any tasks until it is configured. An example
configuration can be found below. Especially the `contracts` section is
important, make sure the `amount` is set to the amount of hosts with which you
want to form a contract. The `allowance` is the amount of money the autopilot
can spend per period, make sure it is not set to zero or contracts won't get
formed.

```json
{
	"hosts": {
		"allowRedundantIPs": false,
		"maxDowntimeHours": 1440,
		"maxConsecutiveScanFailures": 20,
		"scoreOverrides": {}
	},
	"contracts": {
		"set": "autopilot",
		"amount": 50,
		"allowance": "10000000000000000000000000000",
		"period": 6048,
		"renewWindow": 2016,
		"download": 1099511627776, // 1TiB
		"upload": 1099511627776, // 1TiB
		"storage": 1099511627776 // 1TiB
	}
}
```

### Contract Set

The contract set settings on the bus allow specifying a default contract set.
This contract set will be returned by the `bus` through the upload parameters,
and decides what contracts data is upload or migrated to by default. This
setting does not have a default value, it can be updated using the settings API:

- `GET /api/bus/setting/contractset`
- `PUT /api/bus/setting/contractset`

```json
{
        "default": "autopilot"
}
```

In most cases the default set should match the set from your autopilot
configuration in order for migrations to work properly. The contract set can be
overridden by passing it as a query string parameter to the worker's upload and
migrate endpoints.

- `PUT /api/worker/objects/foo?contractset=foo`

### Redundancy

The default redundancy on mainnet is 30-10, on testnet it is 6-2. The redundancy
can be updated using the settings API:

- `GET /api/bus/setting/redundancy`
- `PUT /api/bus/setting/redundancy`

The redundancy can also be passed through query string parameters on the upload
endpoint in the worker API:

- `PUT /api/worker/objects/foo?minshards=2&totalshards=5`

### Gouging

The default gouging settings are listed below. The gouging settings can be
updated using the settings API:

- `GET /api/bus/setting/gouging`
- `PUT /api/bus/setting/gouging`

```json
{
	"hostBlockHeightLeeway": 6,                                   // 6 blocks
	"maxContractPrice": "15000000000000000000000000",             // 15 SC per contract
	"maxDownloadPrice": "3000000000000000000000000000",           // 3000 SC per 1 TB
	"maxRPCPrice": "1000000000000000000000",                      // 1mS per RPC
	"maxStoragePrice": "631593542824",                            // 3000 SC per TB per month
	"maxUploadPrice": "3000000000000000000000000000",             // 3000 SC per 1 TB
	"migrationSurchargeMultiplier": 10,                           // overpay up to 10x for sectors migrations on critical slabs
	"minAccountExpiry": 86400000000000,                           // 1 day
	"minMaxEphemeralAccountBalance": "1000000000000000000000000", // 1 SC
	"minPriceTableValidity": 300000000000                         // 5 minutes
}
```

### Blocklist

Unfortunately the Sia blockchain is subject to hosts that announced themselves
with faulty parameters and/or bad intentions, something which is unavoidable of
course in a decentralized environment. We added a blocklist to give users the
power to block hosts by IP or domain.

- `GET /api/bus/hosts/blocklist`
- `PUT /api/bus/hosts/blocklist`

The Sia Foundation does not ship `renterd` with a default blocklist, the
following entries exclude a decent amount of bad/old/malicious hosts:

- 45.148.30.56
- 51.158.108.244
- siacentral.ddnsfree.com
- siacentral.mooo.com
