plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.aiAppProtectionConfigServiceApi)
  api(commonLibs.grpc.api)

  implementation(projects.configUtils)
  implementation(projects.featureCachingClient)
  implementation(projects.rateLimitingConfigServiceApi)
  implementation(projects.customSignatureConfigServiceApi)
  implementation(projects.anomalyConfigServiceApi)
  implementation(projects.anomalyConfigServiceImpl)
  implementation(projects.entityFetcherCache)
  implementation(projects.dataClassificationConfigServiceApi)
  implementation(commonLibs.traceable.protection.engine.processor.data.type)
  implementation(commonLibs.traceable.protection.engine.config.customsignature)

  implementation(commonLibs.guice7)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.framework.metrics.jakarta)
  implementation(commonLibs.hypertrace.kafkaStreams.eventListener)
  implementation(localLibs.hypertrace.configservice.changeeventapi)
  implementation(commonLibs.traceable.protection.rules.aiapp)
  implementation(commonLibs.traceable.protection.engine.config.aifirewall)
  implementation(commonLibs.traceable.protection.engine.processing.common)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(commonLibs.grpc.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
  testAnnotationProcessor(commonLibs.lombok)
  testCompileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
