plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.labelApplicationRuleConfigServiceApi)
  implementation(projects.configServiceApi)
  implementation(projects.objectStore)
  implementation(projects.validationUtils)
  implementation(projects.configProtoConverter)
  implementation(projects.configServiceChangeEventGenerator)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(testFixtures(projects.configServiceApi))
}

tasks.test {
  useJUnitPlatform()
}
