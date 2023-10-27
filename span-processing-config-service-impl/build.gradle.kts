plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.spanProcessingConfigServiceApi)
  implementation(projects.configServiceApi)
  implementation(projects.objectStore)
  implementation(projects.validationUtils)
  implementation(projects.configProtoConverter)
  implementation(projects.configServiceChangeEventGenerator)

  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.google.re2j)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)

  implementation(commonLibs.guice)
  implementation(commonLibs.guava)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.typesafe.config)

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
