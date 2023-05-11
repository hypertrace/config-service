plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(libs.grpc.api)
  api(libs.typesafe.config)
  implementation(projects.astConfigServiceApi)
  implementation(libs.guice)
  implementation(libs.slf4j.api)

  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.configservice.protoconverter)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
