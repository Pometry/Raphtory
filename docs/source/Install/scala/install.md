# Raphtory - Scala

Raphtory is a graph analytics platform written in the
[Scala](https://www.scala-lang.org) programming language, 
which runs on the JVM (Java virtual machine). 

As such both Java and Scala are required to run Raphtory. 

We additionally require SBT, the scala build tool, to compile 
and run your first Raphtory project.

## Requirements

- Java 11
- SBT

## Installing Java, Scala and SBT

_If you have java installed you can skip this step._ 

Java, Scala and SBT are all very easy to install. 

We will use [SDK Man](https://sdkman.io/) to manage our java insall. 

Please install SDK Man by following their website. 

We will use SDK man to install Java 11, Scala 13 and the latest version of SBT. 

Install by running the following commands

```bash
sdk install java 11.0.11.hs-adpt
sdk install scala 2.13.7
sdk install sbt 1.6.2
```

Test these have been installed by running the `--version` argument 

```bash 
java --version
scala -version
```

If the correct version has not been set as default you can do this explicitly via sdkman. 
This is also how you can change back to another version of these libraries for other projects.

```bash 
sdk use java 11.0.11.hs-adpt
sdk use scala 2.13.7
sdk use sbt 1.6.2
```

## Installing Raphtory

All example projects can be found in the [Raphtory repo](https://github.com/Raphtory/Raphtory).

Let's clone the Raphtory repository using Git and checkout into the latest development version

```bash
git clone https://github.com/Raphtory/Raphtory.git
git checkout 
```


Everything should now be installed and ready for us to get your first Raphtory Job underway!
