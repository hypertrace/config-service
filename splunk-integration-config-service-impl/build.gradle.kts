

plugins {
  `java-library`
  id("com.google.protobuf") version "0.8.17"
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.splunkIntegrationConfigServiceApi)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.guice)
  implementation(projects.configUtils)
  implementation(libs.slf4j.api)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
