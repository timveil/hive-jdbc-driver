todo:
* really need to unify properties.  when using zookeeper, returned properties don't match property names of driver.
* move to JUL logging

bugs:
HiveConf.HIVE_SERVER2_USE_SSL("hive.server2.use.SSL", false,"Set this to true for using SSL encryption in HiveServer2.")
    key should not be uppercase.




old properties before refactor:

    HOST("host","",""),
    DATABASE_NAME("database", "", "default"),
    USER("user","", ""),
    PASSWORD("password","",""),
    //HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE: transport mode must match whats on the server.  should only be used to determine what kind of client to create, this isn't something that is "set" on the server.
    TRANSPORT_MODE("transport.mode", "", TransportMode.binary.toString()),

    // todo: i think these need to be prefixed (like jdbc.) these should not be confused with actual hive conf parameters even thought they may result in the same value

    //HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION
    AUTHENTICATION_TYPE("authentication.type", "", ""),

    KERBEROS_PRINCIPAL("kerberos.principal","", ""),
    //https://community.hortonworks.com/questions/22154/hive-sasl-qop-setting-on-client-and-server.html
    // only applicable for kerberos
    //HiveConf.ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP
    KERBEROS_SASL_QOP("kerberos.sasl.qop","", ""),

    // only applicable in http mode
    SSL_TRUSTSTORE_PATH("ssl.truststore.path","", ""),
    SSL_TRUSTSTORE_PASSWORD("ssl.truststore.password", "", ""),
    SSL_KEYSTORE_PATH("ssl.keystore.path", "", ""),
    SSL_KEYSTORE_PASSWORD("ssl.keystore.password", "", ""),
    // only applicable in http mode
    COOKIE_NAME("cookie.name", "", "hive.server2.auth"),
    //HiveConf.ConfVars.HIVE_SERVER2_ZOOKEEPER_NAMESPACE
    ZOOKEEPER_DISCOVERY_NAMESPACE("zookeeper.discovery.namespace", "", "hiveserver2"),

    PORT_NUMBER("port", "", 10000),

    //the current query timeout limit in seconds; zero means there is no limit
    STATEMENT_QUERY_TIMEOUT("statement.query.timeout", "", 0),

    //zero means there is no limit
    STATEMENT_MAX_ROWS("statement.max.rows", "", 0),

    //If the value specified is zero, then the hint is ignored. should be default for resultset
    STATEMENT_FETCH_SIZE("statement.fetch.size", "", 1000),

    ZOOKEEPER_DISCOVERY_RETRY("zookeeper.discovery.retry", "", 1000);

        SSL_ENABLED("ssl.enabled", "", false),
        KERBEROS_PRE_AUTHENTICATED_SUBJECT_ENABLED("kerberos.subject.enabled", "", false),
        ZOOKEEPER_DISCOVERY_ENABLED("zookeeper.discovery.enabled", "", false),
        COOKIE_REPLAY_ENABLED("cookie.replay.enabled", "", true),