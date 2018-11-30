---
Disclaimer: MongoDB is a registered trademark of MongoDB, Inc.
---

# Configuration

## The configuration file

The `fdbdoc` server process is run and monitored on each server by the
`fdbdocmonitor` daemon. `fdbdocmonitor` and `fdbdoc` itself are
controlled by the `document.conf` file located at

  - `/etc/foundationdb/document/document.conf` on Linux
  - `/usr/local/etc/foundationdb/document/document.conf` on macOS

The `document.conf` file contains several sections, detailed below. Note
that the presence of individual `[fdbdoc.<ID>]` sections actually causes
`fdbdoc` processes to be run.

Whenever the `document.conf` file is modified, the `fdbdocmonitor`
daemon automatically detects the changes and starts, stops, or restarts
child processes as necessary.

Do not attempt to stop FoundationDB services by removing the
configuration file. Removing the file will not stop the services; it
will merely remove your ability to control them in the manner supported
by FoundationDB. During normal operation, services can be stopped by
commenting out or removing the relevant sections of the configuration
file. You can also disable a service at the operating system level or by
removing the software.

### `[fdbmonitor]` section

```ini
## document.conf 
##
## Configuration file for FoundationDB Document Layer server processes 
## Full documentation is available in the FoundationDB Document Layer Configuration document.

[fdbmonitor]
user = foundationdb
group = foundationdb
```

Contains basic configuration parameters of the `fdbdocmonitor` process.
`user` and `group` are used on Linux systems to control the privilege
level of child processes.

### `[general]` section

```ini
[general]
restart_delay = 60
cluster_file = /etc/foundationdb/fdb.cluster
```

Contains settings applicable to all processes.

  - `restart_delay`: Specifies the number of seconds that
    `fdbdocmonitor` waits before restarting a failed process.
  - `cluster_file`: Specifies the location of the cluster file. This
    file and the directory that contains it must be writable by all
    processes (i.e. by the user or group set in the \[fdbmonitor\]
    section). Further details on [specifying the cluster
    file](https://apple.github.io/foundationdb/administration.html#specifying-the-cluster-file)
    are given in the Administration document for the Key-Value Store.

### `[fdbdoc]` section

```ini
## Default parameters for individual fdbdoc processes
[fdbdoc]
command = /usr/sbin/fdbdoc
listen_address = 127.0.0.1:$ID
logdir = /var/log/foundationdb/document
# root-directory = document
# logsize = 10Mib
# maxlogssize = 100MiB
# implicit-transaction-max-retries = 3
# implicit-transaction-timeout = 7000
# pipeline = aggressive
# slow-query-log = all
```

Contains default parameters for all `fdbdoc` processes on this machine.
These same options can be overridden for individual processes in their
respective `[fdbdoc.<ID>]` sections. In this section, the ID of the
individual `fdbserver` can be substituted by using the `$ID` variable in
the value. For example, `listen_address = 127.0.0.1:$ID` makes each
`fdbdoc` listen on a port equal to its ID.


```
  -l ADDRESS Listen address, specified as `[IP_ADDRESS:]PORT' (defaults
             to 127.0.0.1:27016).
  -C CONNFILE
             The path of a file containing the connection string for the
             FoundationDB cluster. The default is first the value of the
             FDB_CLUSTER_FILE environment variable, then `./fdb.cluster',
             then `/etc/foundationdb/fdb.cluster'.
  -d NAME    Name of the directory (managed by the Directory Layer) in the
             Key-Value Store which the Document Layer will use to store all
             of its state.
  -L PATH    Store log files in the given folder (default is `.').
  --loggroup LOGGROUP
             Log Group to be used for logs (default is `default').
  -Rs SIZE   Roll over to a new log file after the current log file
             exceeds SIZE bytes. The default value is 10MiB.
  --maxlogssize SIZE
             Delete the oldest log file when the total size of all log
             files exceeds SIZE bytes. If set to 0, old log files will not
             be deleted. The default value is 100MiB.
  --pipeline OPTION
             Set to `compat' to enable pipelining compatibility mode. This
             mode is both slower and more difficult to use correctly than
             the default. It should only be turned on for compatibility purposes.
  --implicit-transaction-max-retries NUMBER
             Set the maximum number of times that transactions will be retried.
             Defaults to 3. If set to -1, will disable the retry limit.
  --implicit-transaction-timeout NUMBER
             Set a timeout in milliseconds for transactions. Defaults to 7000.
             If set to 0, will disable all timeouts.
  --metric_plugin PATH
             The path of the metric plugin dynamic library to load during runtime.
  --metric_plugin_config PATH
             The path to the configuration file of the plugin.
  -V         Enable verbose logging.
  -h         Display this help message and exit.
  -v         Print version information and exit.

SIZE parameters may use one of the multiplicative suffixes B=1, KB=10^3,
KiB=2^10, MB=10^6, MiB=2^20, GB=10^9, GiB=2^30, TB=10^12, or TiB=2^40.
```

### `[fdbdoc.<ID>]` section(s)

```ini
## An individual fdbdoc process with id 27016
## Parameters set here override defaults from the [fdbdoc] section
[fdbdoc.27016]
```

Each section of this type represents a `fdbdoc` process that will be
run. IDs cannot be repeated. Frequently, an administrator will choose to
run one `fdbdoc` per CPU core. Parameters set in this section apply to
only a single fdbdoc process, and overwrite the defaults set in the
`[fdbdoc]` section. Note that by default, the ID specified in this
section is also used as the network port.
