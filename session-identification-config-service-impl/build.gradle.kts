plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(commonLibs.typesafe.config)
  api(commonLibs.grpc.api)
  api(localLibs.hypertrace.configservice.changeeventgenerator)
  api(projects.featureCachingClient)
  implementation(projects.sensitiveDataConfigServiceApi)
  implementation(projects.configUtils)
  implementation(projects.externalAgentAttributeConfigServiceApi)
  implementation(projects.sessionIdentificationConfigServiceApi)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.hypertrace.grpcutils.context)

  implementation(commonLibs.guice)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.uuidcreator)
  implementation(commonLibs.hypertrace.grpcutils.client)

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
