# README

todo: need a full comparison of size and dependency graph for both hive 1.x and 2.x; a chart would be cool here

## Rationale

### Current Problems

* Bundled Driver Dependency Graph
  * Poorly managed transitive dependencies.  Unnecessary and Unused
  * Size in MB
  * contains server side dependencies
* Unnecessary complexity
  * don't need embedded logic
  * don't need server side code
  * relies on UGI - see book (https://www.gitbook.com/book/steveloughran/kerberos_and_hadoop/details)
* Poorly documented
* Kerberos options are limited, require many outside the driver configs
* URL options are inconsistent in name and style
* URL parsing made overly complex by 
* Logging is sparse and improperly configured
* difficult to embed in applications and 3rd party tools like DbViz, etc.

### Benefits
* Extremely limited Hive/Hadoop dependencies thus greatly trimming the dependency graph and size
* No UGI
* Simplified implementation
* Better documentation
* Simpler and more complete URL config options
* Improved logging
* More options for kerberos
* separate driver's for binary, binary-zookeeper, http, http-zookeeper
* much easier to embed in applications or 3rd party tools (single, small driver, no external configuration)
