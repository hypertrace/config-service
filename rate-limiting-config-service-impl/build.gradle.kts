plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.rateLimitingConfigServiceApi)
  api(localLibs.hypertrace.configservice.api)
  api(projects.customSignatureConfigServiceApi)
  api(projects.dataClassificationConfigServiceApi)

  implementation(projects.customSignatureConfigServiceImpl)
  implementation(projects.modsecurityUtils)
  implementation(projects.anomalyConfigServiceRegistry)
  implementation(projects.entityFetcherCache)
  implementation(projects.configUtils)
  implementation(projects.auditUtils)
  implementation(projects.modsecurityUtils)
  implementation(projects.featureCachingClient)
  implementation(projects.traceableEdgeDecisionConverterUtils)
  implementation(commonLibs.traceable.actorservice.api)

  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.guice7)
  implementation(commonLibs.re2j)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(commonLibs.traceable.platform.ipUtils)
  implementation(commonLibs.traceable.modsecurity.jni)
  implementation(commonLibs.traceable.traceenricher.constants)
  implementation(commonLibs.commons.lang)
  implementation(commonLibs.commons.io)

  implementation(commonLibs.protobuf.javautil)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(commonLibs.hypertrace.grpcutils.client)
  testImplementation(commonLibs.grpc.core)
  testImplementation(commonLibs.hypertrace.grpcutils.client)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
