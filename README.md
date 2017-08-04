# Hive JDBC Driver

This project is alternative to the JDBC driver that is bundled with the Apache Hive project.  The desire to build this grew out of my experience maintaining the Hive JDBC "uber jar" project ([here](https://github.com/timveil/hive-jdbc-uber-jar)) which attempted to produce a smaller, more complete standalone driver jar by crafting an alternative Maven `pom` file.  While that effort mostly succeed in creating a slightly smaller jar, I felt like more could be done to improve the Hive JDBC experience.

As I started building out this project I realized that I wanted to deviate significantly from the existing Apache implementation.  As a result, this project does not desire or attempt to be URL or even feature compatible with the existing Apache Driver.  One obvious manifestation of this is that existing JDBC connection strings/URLs that work with the Apache Driver __WILL NOT WORK__ with this driver without modification.  I've provided a mapping for existing URL properties [here](DRIVER-PROPERTIES.md) as well as plenty of [examples](EXAMPLES.md). Another significant deviation is packaging.  In an effort to significantly trim jar sizes, I publish different flavors of the jar depending on connection type.  There is no one size fits all jar, instead the following flavors are produced:

- Binary: for `binary` connections without zookeeper discovery support
- Binary + Zookeeper: for `binary` connections that leverage zookeeper discovery
- HTTP: for `http` connections without zookeeper discovery support
- HTTP + Zookeeper: for `http` connections that leverage zookeeper discovery

The third significant deviation is the absence of Hadoop or Hive dependencies and their transitive dependency graphs.  The only bridge to Hive in this driver is the Thrift Interface Description Language (IDL) file.  All necessary code was rewritten from the ground up with an emphasis on eliminating external dependencies.  This, in combination with packaging, has the clear benefit of significantly reducing jar sizes.  For example if you are connecting to Hive running LLAP (Hive 2.x) using `binary` transport (the default) and not using Zookeeper discovery, the jar is 92% smaller than the original!

![](docs/sizes.png)



