plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(commonLibs.grpc.api)
  api(commonLibs.typesafe.config)
  api(localLibs.hypertrace.configservice.changeeventgenerator)

  implementation(projects.configProtoUtils)
  implementation(projects.configUtils)
  implementation(projects.fraudDatamodelDerivationConfigServiceApi)
  implementation(projects.fraudDatamodelEventKindConfigServiceApi)
  implementation(projects.fraudDatamodelEventKindConfigServiceImpl)
  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.jackson.databind)
  implementation(commonLibs.jackson.yaml)

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
