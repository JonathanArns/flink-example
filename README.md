# A Flink example project using Java and Gradle.

To package your job for submission to Flink, use: ``gradle shadowJar``. Afterwards, you'll find the
jar to use in the 'build/libs' folder.

Make sure to use Java 8 as the Project-SDK.

To start Kafka and a Flink cluster run:

```
docker-compose up
```
