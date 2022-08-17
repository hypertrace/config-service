plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.licenseStatusConfigServiceApi)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.guice)
  implementation(libs.guava)
  implementation(libs.protobuf.javautil)
  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.configservice.protoconverter)
  // https://traceableai.atlassian.net/browse/ENG-20659
  // This is temporary. Remove this once license enforcer changes are in place
  implementation(libs.traceable.licensemetering.api)
  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)
  implementation(libs.hypertrace.framework.metrics)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
