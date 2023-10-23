plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.partitionerConfigServiceApi)
  implementation(commonLibs.protobuf.javautil)

  implementation(commonLibs.guice)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.hypertrace.documentstore)

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
