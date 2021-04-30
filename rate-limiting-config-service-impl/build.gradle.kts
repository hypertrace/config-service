plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":rate-limiting-config-service-api"))
  api("org.hypertrace.config.service:config-service-api")
  implementation("com.typesafe:config:1.4.1")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.4.0")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.4.0")
  implementation("org.hypertrace.config.service:config-proto-converter")

  annotationProcessor("org.projectlombok:lombok:1.18.20")
  compileOnly("org.projectlombok:lombok:1.18.20")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.9.0")
  testImplementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.4.0")
  testImplementation("org.apache.commons:commons-lang3:3.11")
  testImplementation("io.grpc:grpc-testing:1.37.0")
}

tasks.test {
  useJUnitPlatform()
}
