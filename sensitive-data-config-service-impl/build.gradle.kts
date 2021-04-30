plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":sensitive-data-config-service-api"))
  implementation("org.hypertrace.config.service:config-service-api")
  implementation("ai.traceable.platform:insights-service-api:0.29.5")
  implementation("com.google.guava:guava:30.1-jre")
  implementation("com.google.protobuf:protobuf-java-util:3.15.8")
  implementation("com.typesafe:config:1.4.1")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.3.3")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.3.3")
  implementation("org.hypertrace.config.service:config-proto-converter")

  annotationProcessor("org.projectlombok:lombok:1.18.18")
  compileOnly("org.projectlombok:lombok:1.18.18")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.0")
  testImplementation("org.mockito:mockito-core:3.7.7")
  testImplementation("io.grpc:grpc-testing:1.37.0")
  testImplementation(testFixtures("org.hypertrace.config.service:config-service-api"))
  testImplementation(testFixtures(project(":traceable-config-service")))
  testAnnotationProcessor("org.projectlombok:lombok:1.18.18")
  testCompileOnly("org.projectlombok:lombok:1.18.18")
}

tasks.test {
  useJUnitPlatform()
}
