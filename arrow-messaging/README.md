## Test
Below are the list of commands in order to run unit/integration tests and create coverage report.
```bash
sbt:arrow-messaging> clean
sbt:arrow-messaging> compile
sbt:arrow-messaging> coverage
sbt:arrow-messaging> test
sbt:arrow-messaging> it:test
sbt:arrow-messaging> coverageReport
```

## Publish Jar
```bash
sbt publishLocal
```
