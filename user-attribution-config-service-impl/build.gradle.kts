plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.userAttributionConfigServiceApi)
  implementation(libs.hypertrace.configservice.api)
  implementation(projects.configUtils)
  implementation(projects.externalAgentAttributeConfigServiceApi)
  implementation(libs.guice)
  implementation(libs.guava)
  implementation(libs.re2j)
  implementation(libs.protobuf.javautil)
  implementation(libs.slf4j.api)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(libs.jackson.yaml)
  implementation(libs.re2j)
  implementation(libs.protobuf.javautil)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockito.junit)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
