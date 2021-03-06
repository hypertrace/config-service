plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(project(":spaces-config-service-api"))
  implementation(project(":config-service-api"))
  implementation(project(":config-proto-converter"))
  implementation("com.google.guava:guava:30.1.1-jre")
  implementation("io.reactivex.rxjava3:rxjava:3.0.11")
  implementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.4.0")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.4.0")
  implementation("org.hypertrace.core.grpcutils:grpc-server-rx-utils:0.4.0")
  implementation("org.hypertrace.core.grpcutils:grpc-client-rx-utils:0.4.0")

  testImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  testImplementation("org.mockito:mockito-core:3.9.0")
  testImplementation("org.mockito:mockito-junit-jupiter:3.9.0")
  testImplementation(testFixtures(project(":config-service-api")))
}

tasks.test {
  useJUnitPlatform()
}
