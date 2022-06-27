plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.spanProcessingConfigServiceApi)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.protobuf.javautil)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)

  implementation(libs.guice)
  implementation(libs.guava)
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
