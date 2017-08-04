# hive-jdbc

bugs:
HiveConf.HIVE_SERVER2_USE_SSL("hive.server2.use.SSL", false,"Set this to true for using SSL encryption in HiveServer2.")
    key should not be uppercase.



hive-jdbc-1.2.1000.2.5.3.0-37-standalone.jar is 20,103,967 bytes

hive-jdbc-1.2.1000.2.6.0.3-8-standalone.jar is 19,955,690 bytes

hive versions in 2.6:

hive 1:
1.2.1000.2.6.0.3-8

hive 2:
2.1.0.2.6.0.3-8

```
HIVE_CLI_SERVICE_PROTOCOL_V1,

// V2 adds support for asynchronous execution
HIVE_CLI_SERVICE_PROTOCOL_V2

// V3 add varchar type, primitive type qualifiers
HIVE_CLI_SERVICE_PROTOCOL_V3

// V4 add decimal precision/scale, char type
HIVE_CLI_SERVICE_PROTOCOL_V4

// V5 adds error details when GetOperationStatus returns in error state
HIVE_CLI_SERVICE_PROTOCOL_V5

// V6 uses binary type for binary payload (was string) and uses columnar result set
HIVE_CLI_SERVICE_PROTOCOL_V6

// V7 adds support for delegation token based connection
HIVE_CLI_SERVICE_PROTOCOL_V7

// V8 adds support for interval types
HIVE_CLI_SERVICE_PROTOCOL_V8

// V9 adds support for serializing ResultSets in SerDe
HIVE_CLI_SERVICE_PROTOCOL_V9

// V10 adds support for in place updates via GetOperationStatus
HIVE_CLI_SERVICE_PROTOCOL_V10
```

```
 Options are NONE (uses plain SASL), NOSASL, KERBEROS, LDAP, PAM and CUSTOM.
Set following for KERBEROS mode:
```


https://community.hortonworks.com/articles/28537/user-authentication-from-windows-workstation-to-hd.html

http://web.mit.edu/kerberos/dist/index.html

C:\Program Files\MIT\Kerberos

Copy the krb5.conf file (from the HDP KDC) to above mentioned location and rename krb5.conf to krb5.ini.


had to specify -Djava.security.krb5.conf=C:\ProgramData\MIT\Kerberos5\krb5.ini when starting up dbviz, setting environment variable, KRB5_CONFIG, did not work.

must specify Djavax.security.auth.useSubjectCredsOnly=false for things to work. need to investigate why


this is a good reference: https://github.com/jcmturner/java-kerberos-utils


NO SUPPORT for IBM JDK


// good sql to help look at date issues in hive
select col_date, date_format(col_date, 'EEE, d MMM yyyy HH:mm:ss Z'), col_timestamp, date_format(col_timestamp, 'EEE, d MMM yyyy HH:mm:ss Z') from jdbc_test.date_time_test



## Rationale

### Current Problems

* Bundled Driver Dependency Graph
  * Poorly managed transitive dependencies
    * Unnecessary and unused jars abound (this is a problem with hadoop projects in general, not just hive)
    * outdated or duplicate transitive dependencies can/do cause major challenges when embedding driver into applications or tools
  * Size in MB
    * hive-jdbc-2.1.0.2.6.1.0-129-standalone.jar (the latest Hive 2.0) is over 66mb
    * hive-jdbc-1.2.1000.2.6.1.0-129-standalone.jar (the latest 1.2.x) is almost 20mb
  * contains server side dependencies
* Unnecessary complexity
  * don't need embedded logic
  * don't need server side code
  * relies on UGI - see book (https://www.gitbook.com/book/steveloughran/kerberos_and_hadoop/details)
* Poorly documented
* Kerberos options are limited, require many outside the driver configs
* URL options are inconsistent in name and style
* URL parsing made overly complex by different delimeters and overloaded meanings for some properties or combinations of properties
* Logging is sparse and improperly configured
* difficult to embed in applications and 3rd party tools like DbViz, etc.

### Benefits
* Extremely limited dependency graph thus trimming the jar size and reducing classpath risks/issues
    * driver-binary-1.0-SNAPSHOT.jar is 4.9mb
    * driver-binary-zookeeper-1.0-SNAPSHOT.jar is 8.3mb
    * driver-http-1.0-SNAPSHOT.jar is 6.1mb
    * driver-http-zookeeper-1.0-SNAPSHOT.jar is 9.4mb
* Only dependency on Hive project is the *.thrift file that it defines.  all thrift client code is compiled directly in this project.
* No UGI; reimplementaiton of all kerberos/jaas related code
* Simplified implementation
* Better documentation
* Simpler and more complete URL config options
* Improved logging
* More options for kerberos
* separate driver's for binary, binary-zookeeper, http, http-zookeeper
* much easier to embed in applications or 3rd party tools (single, small driver jar. no external configuration)
