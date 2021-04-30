plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":config-service-api"))

  implementation("com.fasterxml.jackson.core:jackson-databind:2.12.2")
  implementation("com.google.guava:guava:30.1.1-jre")
  implementation("com.google.protobuf:protobuf-java-util:3.15.7")
  implementation("com.typesafe:config:1.4.1")
  implementation("org.slf4j:slf4j-api:1.7.30")

  implementation("org.hypertrace.core.documentstore:document-store:0.5.4")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.4.0")

  annotationProcessor("org.projectlombok:lombok:1.18.20")
  compileOnly("org.projectlombok:lombok:1.18.20")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.9.0")
  testImplementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.4.0")
}

tasks.test {
  useJUnitPlatform()
}
