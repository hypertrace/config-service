plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.reportingConfigServiceApi)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.guice)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(commonLibs.uuidcreator)
  implementation(projects.configUtils)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
