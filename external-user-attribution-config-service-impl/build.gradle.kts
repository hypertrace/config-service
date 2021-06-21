plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":external-user-attribution-config-service-api"))
  implementation(project(":user-attribution-config-service-api"))
  implementation("com.google.inject:guice:5.0.1")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.5.0")
  implementation("com.github.f4b6a3:uuid-creator:2.7.11")

  annotationProcessor("org.projectlombok:lombok:1.18.20")
  compileOnly("org.projectlombok:lombok:1.18.20")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.9.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.9.0")
  testImplementation("com.google.protobuf:protobuf-java-util:3.15.8")

  testAnnotationProcessor("org.projectlombok:lombok:1.18.20")
  testCompileOnly("org.projectlombok:lombok:1.18.20")
}

tasks.test {
  useJUnitPlatform()
}
