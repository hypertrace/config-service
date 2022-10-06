plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.notificationChannelConfigServiceApi)
  implementation(projects.configServiceApi)
  implementation(projects.configProtoConverter)
  implementation(projects.objectStore)
  implementation(projects.validationUtils)
  implementation(projects.configServiceChangeEventGenerator)

  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.slf4j.api)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.protobuf.javautil)
  testImplementation(testFixtures(projects.configServiceApi))
}

tasks.test {
  useJUnitPlatform()
}
