plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.notificationRuleConfigServiceApi)
  implementation(projects.configServiceApi)
  implementation(projects.configProtoConverter)
  implementation(projects.configObjectStore)
  implementation(projects.configValidationUtils)
  implementation(projects.configServiceChangeEventGenerator)

  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.slf4j2.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(testFixtures(projects.configServiceApi))
}

tasks.test {
  useJUnitPlatform()
}
