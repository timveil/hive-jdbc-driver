# README

todo: need a full comparison of size and dependency graph for both hive 1.x and 2.x; a chart would be cool here

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
* Extremely limited dependency graph thus trimming the jar size and complexity
    * driver-binary-1.0-SNAPSHOT.jar is 4.9mb
    * driver-binary-zookeeper-1.0-SNAPSHOT.jar is 8.3mb
    * driver-http-1.0-SNAPSHOT.jar is 6.1mb
    * driver-http-zookeeper-1.0-SNAPSHOT.jar is 9.4mb
* Only dependency on Hive project is the *.thrift file that it defines.  all thrift client code is compiled directly in this project.
* No UGI
* Simplified implementation
* Better documentation
* Simpler and more complete URL config options
* Improved logging
* More options for kerberos
* separate driver's for binary, binary-zookeeper, http, http-zookeeper
* much easier to embed in applications or 3rd party tools (single, small driver jar. no external configuration)
