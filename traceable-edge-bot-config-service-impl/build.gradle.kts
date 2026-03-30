plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  implementation(projects.configUtils)
  implementation(projects.traceableEdgeBotConfigServiceApi)

  implementation(commonLibs.grpc.api)
  implementation(commonLibs.typesafe.config)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)

  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)

  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
