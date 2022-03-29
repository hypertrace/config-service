plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.externalDataClassificationConfigServiceApi)
  implementation(projects.dataClassificationConfigServiceApi)
  implementation(projects.sensitiveDataConfigServiceApi)
  implementation(projects.externalDataClassificationConfigServiceApi)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.uuidCreator)
  implementation(libs.guice)
  implementation(libs.slf4j.api)
  implementation(libs.protobuf.javautil)
  implementation(projects.configUtils)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
  testImplementation(libs.protobuf.javautil)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
