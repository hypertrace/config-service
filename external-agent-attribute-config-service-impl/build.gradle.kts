plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.externalAgentAttributeConfigServiceApi)
  api(commonLibs.typesafe.config)
  api(projects.featureCachingClient)
  implementation(projects.userAttributionConfigServiceApi)
  implementation(projects.sessionIdentificationConfigServiceApi)
  implementation(projects.authDetectionConfigServiceApi)
  implementation(projects.jwtExtractionConfigServiceApi)
  implementation(projects.traceableSpanProcessingConfigServiceApi)
  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.uuidcreator)
  implementation(commonLibs.protobuf.javautil)
  implementation(projects.configUtils)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(commonLibs.protobuf.javautil)
  testImplementation(commonLibs.guava)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))

  testAnnotationProcessor(commonLibs.lombok)
  testCompileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
