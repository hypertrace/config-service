plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":blocking-config-service-api"))
  api(project(":region-config-service-api"))
  api("ai.traceable.platform:opa-aggregator-api:0.1.45")

  implementation("com.google.inject:guice:5.0.1")
  implementation("com.google.guava:guava:30.1-jre")
  implementation("com.google.protobuf:protobuf-java-util:3.15.6")
  implementation("com.typesafe:config:1.4.1")
  implementation("org.slf4j:slf4j-api:1.7.30")

  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.3.4")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.3.4")

  annotationProcessor("org.projectlombok:lombok:1.18.18")
  compileOnly("org.projectlombok:lombok:1.18.18")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.8.0")
  testImplementation("io.grpc:grpc-testing:1.36.0")
  testAnnotationProcessor("org.projectlombok:lombok:1.18.18")
  testCompileOnly("org.projectlombok:lombok:1.18.18")
}

tasks.test {
  useJUnitPlatform()
}
