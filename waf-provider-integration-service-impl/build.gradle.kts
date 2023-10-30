

plugins {
  `java-library`
  alias(commonLibs.plugins.google.protobuf)
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.wafProviderIntegrationServiceApi)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.guice)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
