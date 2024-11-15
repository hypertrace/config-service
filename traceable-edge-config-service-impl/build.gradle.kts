plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  implementation(projects.configUtils)
  implementation(projects.traceableEdgeConfigServiceApi)
  implementation(projects.traceableEdgeBotConfigServiceApi)
  implementation(projects.traceableEdgeDecisionConfigServiceApi)

  implementation(commonLibs.grpc.api)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.re2j)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)

  implementation(commonLibs.guice)
  implementation(commonLibs.slf4j2.api)

  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.framework.metrics)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.traceable.actorservice.api)

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
