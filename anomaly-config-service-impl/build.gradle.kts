plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":anomaly-config-service-api"))
  implementation("org.hypertrace.config.service:config-service-api")

  implementation("com.google.inject:guice:5.0.1")
  implementation("com.google.guava:guava:30.1.1-jre")
  implementation("com.google.protobuf:protobuf-java-util:3.15.8")
  implementation("com.typesafe:config:1.4.1")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("org.apache.commons:commons-csv:1.8")
  implementation("com.github.f4b6a3:uuid-creator:2.7.11")

  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.4.1")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.4.1")
  implementation("org.hypertrace.config.service:config-proto-converter")

  annotationProcessor("org.projectlombok:lombok:1.18.20")
  compileOnly("org.projectlombok:lombok:1.18.20")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.9.0")
  testImplementation(testFixtures("org.hypertrace.config.service:config-service-api"))
  testAnnotationProcessor("org.projectlombok:lombok:1.18.20")
  testCompileOnly("org.projectlombok:lombok:1.18.20")
}

tasks.test {
  useJUnitPlatform()
}
