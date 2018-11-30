---
Disclaimer: MongoDB is a registered trademark of MongoDB, Inc.
---

# Administration

## Starting and stopping

After installation, the FoundationDB Document Layer is set to start
automatically. You can manually start and stop the layer with the
commands shown below.

These commands start and stop the master `fdbdocmonitor` process, which
in turn starts `fdbdoc` processes.

### Linux

On Linux, the Document Layer is started and stopped using the `service`
command as follows:

```
$ sudo service fdb-document-layer start
$ sudo service fdb-document-layer stop
```

On Ubuntu, it can be prevented from starting at boot as follows (without
stopping the service):

```
$ sudo update-rc.d fdb-document-layer disable
```

On RHEL/CentOS, it can be prevented from starting at boot as follows
(without stopping the service):

```
$ sudo chkconfig fdb-document-layer off 
```

### macOS

On macOS, the Document Layer is started and stopped using `launchctl` as
follows:

```
$ sudo launchctl load /Library/LaunchDaemons/com.foundationdb.fdbdocmonitor.plist
$ sudo launchctl unload /Library/LaunchDaemons/com.foundationdb.fdbdocmonitor.plist
```

It can be stopped and prevented from starting at boot as follows:

```
$ sudo launchctl unload -w /Library/LaunchDaemons/com.foundationdb.fdbdocmonitor.plist
```

## fdbdocmonitor and fdbdoc

The core Document Layer server process is `fdbdoc`. Each `fdbdoc`
process uses up to one full CPU core.

To make configuring, starting, stopping, and restarting `fdbdoc`
processes easy, the Document Layer also comes with a singleton daemon
process, `fdbdocmonitor`, which is started automatically on boot.
`fdbdocmonitor` reads the [document.conf](configuration.md) file and
starts the configured set of `fdbdoc` processes.

Whenever the `document.conf` file changes, the `fdbdocmonitor` daemon
automatically detects the changes and starts, stops, or restarts child
processes as necessary.

During normal operation, `fdbdocmonitor` is transparent, and you
interact with it only by modifying the configuration in [document.conf](configuration.md)
and perhaps occasionally by [starting and stopping](administration.md) it manually. If some problem
prevents an `fdbdoc` process from starting or causes it to stop
unexpectedly, `fdbdocmonitor` will log errors to the system log.

## Managing trace files

By default, trace files are output to:

  ### Linux
  
  `/var/log/foundationdb/document/`
  
  ### macOS
  
  `/usr/local/foundationdb/document/logs/`

Trace files are rolled every 10MB. These files are valuable to the
FoundationDB team for support and diagnostic purposes and should be
retained. Old trace files are automatically deleted so that there
are no more than 100 MB worth of trace files per process. The size of each log file and the
maximum total size of the log files are configurable on a per process basis in the [configuration file](configuration.md).

## Slow query log

If slow query logging has been enabled in the [configuration
file](configuration.md) (as it is by default), then any application queries
that result in full collection scans will be logged to the current trace
file.

If logs are written to the default location, then a list of all slow
queries present in the logs can be recovered with the following command:

  ### Linux
  
  `grep SlowQuery /var/log/foundationdb/document/fdbdoc-trace*`
  
  ### macOS
  
  `grep SlowQuery /usr/local/foundationdb/document/logs/fdbdoc-trace*`


Future versions of the Document Layer may use a different storage format
or storage location for slow query logging. Any changes in this
functionality will be included in the relevant release notes.


## MongoDB® administrative commands

Due to the Document Layer's [unique architecture](architecture.md),
replication and sharding operations are entirely invisible to users and
administrators. A significant benefit of this approach is that the Document
Layer does not need to provide additional commands such as those described 
in `Replication Commands` or `Sharding Commands` in MongoDB®.

Document Layer does not have any authentication framework yet. So, we don't
support any of 'Authentication Commands', 'User Management
Commands,' and 'Role Management Commands'. We are going to add TLS based authentication
very soon.

Among the 'Instance Administration Commands,' the following are
supported: `create`, `dropDatabase`, `dropIndexes`, `drop`, and
`renameCollection`. Many of the remaining commands concern issues with
MongoDB® operation that administrators of the Document Layer don't need
to consider (e.g. `fsync`, `compact`) and so are unlikely to be
implemented unless they prove necessary to provide compatibility with a
framework, ODM, or legacy software.

The following 'Diagnostic Commands' are implemented or partially
implemented: `buildInfo`, `dbStats`, `collStats`, `listDatabases`,
`ping`, `whatsmyuri`. If you would like to see another diagnostic
command implemented or made more detailed, please tell us about it so we
can prioritize this work.

Additionally, the Document Layer has added the following database commands
that are not present in MongoDB®: `beginTransaction`,
`commitTransaction`, `rollbackTransaction`, `getDocLayerVersion`, and
`getKVStatus`.

## Uninstalling

To uninstall the Document Layer from a cluster of one or more machines:

1.  Uninstall the packages on each machine in the cluster.
    
    **Ubuntu**
        
    ```
    $ sudo dpkg -P fdb-document-layer
    ```    
    
    **RHEL/CentOS**
        
    ```
    $ sudo rpm -e fdb-document-layer
    ```

    **macOS**

    ```
    $ sudo /usr/local/foundationdb/document/uninstall-FoundationDB-Document-Layer.sh
    ```    
    
2.  Remove the key-value pairs stored by the Document Layer in the
    Key-Value store. Connect to the appropriate cluster with one of the Key-Value store
    language bindings, and remove the directory which contains the
    state for the Document Layer (defaults to `document`). Here is an example using Python: 

    ```python
    import fdb
    fdb.api_version(200)
    db = fdb.open()
    dl = fdb.DirectoryLayer()
    dl.remove_if_exists(db, ["document"])
    ```
