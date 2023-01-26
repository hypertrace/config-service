plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.partitionerConfigServiceApi)
  implementation(libs.protobuf.javautil)

  implementation(libs.guice)
  implementation(libs.slf4j.api)
  implementation(libs.typesafe.config)
  implementation(libs.hypertrace.documentstore)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
  testImplementation(testFixtures(projects.configServiceApi))
}

tasks.test {
  useJUnitPlatform()
}
