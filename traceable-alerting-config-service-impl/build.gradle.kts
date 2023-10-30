plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.traceableAlertingConfigServiceApi)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(projects.configUtils)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
