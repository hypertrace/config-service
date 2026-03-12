plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  implementation(projects.configUtils)
  implementation(projects.entityFetcherCache)
  implementation(projects.anomalyConfigServiceApi)
  implementation(projects.customSignatureConfigServiceApi)
  implementation(projects.traceableEdgeConfigServiceApi)
  implementation(projects.cloudBotDeploymentConfigServiceApi)
  implementation(projects.traceableEdgeBotConfigServiceApi)
  implementation(projects.traceableEdgeDecisionConfigServiceApi)
  implementation(projects.featureCachingClient)

  implementation(commonLibs.grpc.api)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.traceable.protection.engine.config.webapp)
  implementation(commonLibs.traceable.protection.engine.config.apiprotect)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)

  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.jackson.databind)
  implementation(commonLibs.traceable.protection.rules.filtering)
  implementation(commonLibs.traceable.protection.engine.config.filtering)

  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.framework.metrics.jakarta)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.hypertrace.entitychangeevent.api)
  implementation(commonLibs.hypertrace.kafkaStreams.eventListener)
  implementation(commonLibs.traceable.protection.engine.config.customsignature)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(commonLibs.grpc.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
