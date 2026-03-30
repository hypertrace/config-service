plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(commonLibs.typesafe.config)
  api(commonLibs.grpc.api)
  implementation(projects.configServiceCommons)
  implementation(projects.cloudBotDeploymentConfigServiceApi)
  implementation(projects.configUtils)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(commonLibs.guice7)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.uuidcreator)
  implementation(commonLibs.traceable.platform.ipUtils)

  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.jackson.yaml)

  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.domain.validation)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
