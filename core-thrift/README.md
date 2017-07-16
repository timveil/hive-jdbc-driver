# Core Thrift

This module contains the thrift generated classes based on the TCLIService.thrift file found in the Hive Project.  The only change to this file is the `namespace` so that all files related to this project fall under the same java package.  This does not impact functionality.

```thrift
// current
namespace java veil.hdp.hive.jdbc.core.binding

// original
namespace java apache.hive.service.rpc.thrift
```

I have tried many approaches to generating these files using thrift... from installing and builidng thrift locally to using the maven plugin.  I found both of these overly complicated and error prone.  Luckily, the thrift project now publishes a docker file which contains the required compiler.  This is the approach i have chosen but does reuqire you to install docker.

To generate the java files in this module, you must first install Docker then do the following:

```bash
// run from root of core-thrift module to pull the correct docker image

docker pull thrift:0.9.3
```
Then run the following command to use the docker image to compile the `.thrift` file into java classes.


```bash
docker run -v "$PWD:/data" thrift:0.9.3 thrift --gen java:beans -out /data/src/main/java /data/src/main/resources/TCLIService.thrift
```
