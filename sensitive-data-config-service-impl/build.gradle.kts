plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":sensitive-data-config-service-api"))
  api("org.hypertrace.config.service:config-service-api")
  implementation("com.google.guava:guava:30.0-jre")
  implementation("com.google.protobuf:protobuf-java-util:3.13.0")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.3.2")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.3.2")

  annotationProcessor("org.projectlombok:lombok:1.18.12")
  compileOnly("org.projectlombok:lombok:1.18.12")

  testImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
}

tasks.test {
  useJUnitPlatform()
}