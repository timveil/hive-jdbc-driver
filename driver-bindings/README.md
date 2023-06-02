# Driver Bindings

This module contains the Thrift generated classes based on the TCLIService.thrift file found in the Hive Project.  The only change to this file is the `namespace` so that all files related to this project fall under the same java package.  This does not impact functionality.

```thrift
// current
namespace java veil.hdp.hive.jdbc.bindings

// original
namespace java apache.hive.service.rpc.thrift
```

I have tried many approaches to generate these files using Thrift... from installing and building Thrift locally to using the Maven plugin.  I found both of these overly complicated and error prone.  Luckily, the Thrift project now publishes a Docker file which contains the required compiler.  This is the approach I have chosen but does require you to install Docker.

To generate the java files in this module, you must first install Docker then do the following:

```bash
// run from root of driver-bindings module to pull the correct Docker image

docker pull docker.io/ahawkins/thrift:0.18.1
```
Then run the following command to use the Docker image to compile the `.thrift` file into java classes.


```bash
docker run -v "$PWD:/data" ahawkins/thrift:0.18.1 thrift --gen java:beans,generated_annotations=suppress -out /data/src/main/java /data/src/main/resources/TCLIService.thrift
```
