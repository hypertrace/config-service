plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(commonLibs.grpc.api)
  api(commonLibs.typesafe.config)
  api(localLibs.hypertrace.configservice.changeeventgenerator)

  implementation(projects.configUtils)
  implementation(projects.fraudDatamodelDerivationConfigServiceApi)
  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.protobuf.javautil)

  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)

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
