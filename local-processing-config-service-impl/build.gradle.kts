plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.localProcessingConfigServiceApi)
  api(projects.traceableSpanProcessingConfigServiceApi)
  api(projects.featureCachingClient)
  implementation(projects.configUtils)
  implementation(projects.anomalyConfigServiceApi)
  implementation(projects.customSignatureConfigServiceApi)
  implementation(projects.entityFetcherCache)

  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.spanProcessingUtils)
  implementation(commonLibs.guice)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(localLibs.hypertrace.configservice.span.processing.api)
  implementation(commonLibs.uuidcreator)
  implementation(commonLibs.hypertrace.entityservice.api)
  implementation(commonLibs.traceable.apinaming.model)
  implementation(commonLibs.traceable.platform.deepDataStore)
  implementation(commonLibs.traceable.platform.trainingEvaluationFramework)
  implementation(commonLibs.hypertrace.framework.metrics)

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
