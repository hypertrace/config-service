plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  implementation(projects.configUtils)
  implementation(projects.configProtoUtils)
  implementation(projects.traceableEdgeDecisionConfigServiceApi)
  implementation(projects.externalAgentAttributeConfigServiceApi)
  implementation(projects.userAttributionConfigServiceApi)
  implementation(projects.rateLimitingConfigServiceApi)
  implementation(projects.detectionExclusionConfigServiceApi)

  implementation(commonLibs.grpc.api)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.protobuf.javautil)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(commonLibs.traceable.opadistributor.api)
  implementation(commonLibs.guava)

  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.re2j)

  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.hypertrace.framework.metrics)
  implementation(commonLibs.traceable.actorservice.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(commonLibs.grpc.core)
  testRuntimeOnly(commonLibs.grpc.netty)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
  testImplementation(commonLibs.commons.io)
}

tasks.test {
  useJUnitPlatform()
}
