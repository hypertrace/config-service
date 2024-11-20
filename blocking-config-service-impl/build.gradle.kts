plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.anomalyConfigServiceApi)
  api(projects.blockingConfigServiceApi)
  api(projects.customSignatureConfigServiceApi)
  api(projects.regionConfigServiceApi)
  api(projects.iprangeConfigServiceApi)
  api(projects.rateLimitingConfigServiceApi)
  api(projects.maliciousSourcesConfigServiceApi)
  api(projects.detectionExclusionConfigServiceApi)

  implementation(commonLibs.traceable.opadistributor.api)
  implementation(commonLibs.traceable.actorservice.api)
  implementation(commonLibs.traceable.platform.ipUtils)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(projects.configUtils)
  implementation(projects.modsecurityUtils)

  implementation(commonLibs.guice)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.commons.csv)
  implementation(commonLibs.commons.lang)

  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.entityservice.api)
  implementation(commonLibs.hypertrace.framework.metrics)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.grpc.core)
  testAnnotationProcessor(commonLibs.lombok)
  testCompileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
