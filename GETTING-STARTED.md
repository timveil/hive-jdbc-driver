# Getting Started

To begin using, download the appropriate driver version [here](https://github.com/timveil/hive-jdbc/releases).  Again, there are four flavors of the driver jar corresponding to different connection mechanisms:

- __Binary__: for `binary` connections without zookeeper discovery support
- __Binary + Zookeeper__: for `binary` connections that leverage zookeeper discovery
- __HTTP__: for `http` connections without zookeeper discovery support
- __HTTP + Zookeeper__: for `http` connections that leverage zookeeper discovery

All driver versions use the same JDBC URL format but have different Driver classnames.  The JDBC URL format differs from the Apache Hive version in the way it delimits properties.  For example the general format for connections strings is as follows:

```
// New URL format
jdbc:hive2://[host]:[port]/[database]?[key=value]&[key=value]
```

Please note the following:
* The `database` must follow `host` and `port` and start with a `/`.  It corresponds to [URI.getPath()](https://docs.oracle.com/javase/7/docs/api/java/net/URI.html#getPath()).
* Key/Value pairs follow the `database` name and begin with a `?`.  Additional Key/Value pairs are delimited by `&`.  Key/Value pairs correspond to [URI.getQuery()](https://docs.oracle.com/javase/7/docs/api/java/net/URI.html#getQuery()).

This approach is different from the Apache Hive version which generally uses the following format. 

```
// Apache Hive URL format
jdbc:hive2://[host]:[port]/[database];[key=value];[key=value]
```

Notice the exclusive use of `;` to separate the `database` from Key/Value pairs as well as additional Key/Value pairs.  This approach leads to rather complicated parsing logic without providing a clear benefit.  For more details on the Apache Hive format see [this](https://cwiki.apache.org/confluence/display/Hive/HiveServer2+Clients#HiveServer2Clients-ConnectionURLs).

## Binary Driver

The Binary driver is intended to be used with clusters where `hive.server2.transport.mode` is set to `binary` and Zookeeper discovery is not enabled.  The classname for this driver is `veil.hdp.hive.jdbc.BinaryHiveDriver`



