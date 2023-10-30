plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.detectionExclusionConfigServiceApi)
  api(projects.anomalyConfigServiceImpl)
  api(projects.featureCachingClient)
  api(localLibs.hypertrace.configservice.api)

  implementation(projects.configUtils)

  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.guice)
  implementation(commonLibs.re2j)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.traceable.platform.ipUtils)
  implementation(commonLibs.traceable.anomalydetection.configProviders)
  implementation(commonLibs.traceable.actorservice.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.hypertrace.grpcutils.client)
  testImplementation(commonLibs.commons.lang)
  testImplementation(commonLibs.grpc.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
