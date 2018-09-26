# Hive JDBC Driver

This project is alternative to the JDBC driver that is bundled with the Apache Hive project.  The desire to build this grew out of my experience maintaining the Hive JDBC "uber jar" project ([here](https://github.com/timveil/hive-jdbc-uber-jar)) which attempted to produce a smaller, more complete standalone driver jar by crafting an alternative Maven `pom` file.  While that effort mostly succeed in creating a slightly smaller jar, I felt like more could be done to improve the Hive JDBC experience.

As I started building out this project I realized that I wanted to deviate significantly from the existing Apache implementation.  As a result, this project does not desire or attempt to be URL or even feature compatible with the existing Apache Driver.  One obvious manifestation of this is that existing JDBC connection strings/URLs that work with the Apache Driver __WILL NOT WORK__ with this driver without modification.  I've provided a mapping for existing URL properties [here](DRIVER-PROPERTIES.md#apache-driver-property-mapping) as well as plenty of [examples](EXAMPLES.md). 

Another significant deviation from the Apache implementation is the absence of Hadoop or Hive dependencies and their transitive dependency graphs.  The only bridge to Hive in this driver is the Thrift Interface Description Language (IDL) file and the Java bindings it generates.  All necessary code was rewritten from the ground up with an emphasis on eliminating external dependencies.  This has the clear benefit of significantly reducing jar sizes and reducing opportunities for class conflicts!  See size comparison below:

![](docs/sizes.png)
_the standalone jar for Hive 1.2.x does not contain all necessary dependencies so this is not an accurate representation of the real size_

## Areas of Focus

The following are board areas where I have attempted expand or improve the existing Hive Driver:

* __Jar Size__ - focused on creating a smaller, more portable jar
* __Dependency Graph__ - because JDBC drivers are often embedded in other applications it is important to limit the number of external dependencies that are shaded into the final jar.  Shaded dependencies are often the source of size bloat and classloader conflicts.  Every effort has been made to limit the number of external dependencies.
* __Logging__ - logging inside hadoop dependencies and Hive is often a confusing mix of logging frameworks.  This driver works to provide clearer logging thru the Log4J2 api.
* __JDBC Compatibility__ - it is doubtful that Hive will ever allow true JDBC specification compatibility... the underlying datastore simply doesn't yet (may never) provide many of the required concepts.  Having said that, there are plenty of methods and interfaces within the JDBC spec that have not been implemented by the Apache Driver that could have been.  I've attempted rectify that.
* __Documentation__ - the existing Hive documentation can be difficult to follow.  For example, there doesn't seem to be a good single point of reference for all supported URL parameters.  Instead the complete picture of options must be gleaned from a handful of examples and sources.  This makes setting up connections difficult.
* __Simplification__ - the existing driver supports concepts like "embedded mode" which adds complexity to connection logic and requires server side dependencies.  If you need "embedded mode", this driver is not for you.  
* __External Configuration__ - in my experience you often need to add Java VM options (`-Dsome_config`) to get Hive JDBC working or to enable debugging.  This is especially prevalent when dealing with Kerberos.  This driver moves some of those common configuration flags to URL properties.

## Current State

This project is **pre-alpha** and should be considered **experimental** a this point.  Currently it is built against Hortonworks Repos, but will soon be switched to more closely follow the Apache released versions.