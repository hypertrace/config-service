plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(commonLibs.grpc.api)
  api(commonLibs.typesafe.config)
  api(projects.externalDataClassificationConfigServiceApi)
  api(projects.featureCachingClient)

  implementation(projects.dataClassificationConfigServiceApi)
  implementation(projects.sensitiveDataConfigServiceApi)
  implementation(projects.sessionIdentificationConfigServiceApi)
  implementation(projects.userAttributionConfigServiceApi)
  implementation(commonLibs.traceable.insights.api)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.hypertrace.framework.metrics)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.guice)
  implementation(commonLibs.guava)
  implementation(projects.configUtils)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.protobuf.javautil)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
