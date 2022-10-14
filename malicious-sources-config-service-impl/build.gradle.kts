plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(libs.typesafe.config)
  api(libs.grpc.api)
  implementation(projects.maliciousSourcesConfigServiceApi)
  implementation(projects.configUtils)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(libs.hypertrace.configservice.protoconverter)

  implementation(libs.guice)
  implementation(libs.protobuf.javautil)
  implementation(libs.slf4j.api)
  implementation(libs.hypertrace.grpcutils.client)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)
  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
