plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(commonLibs.grpc.api)
  api(commonLibs.typesafe.config)

  implementation(projects.anomalyConfigServiceRegistry)
  implementation(projects.customSignatureConfigServiceApi)
  implementation(projects.modsecurityUtils)
  implementation(projects.configUtils)
  implementation(projects.auditUtils)
  implementation(projects.featureCachingClient)
  implementation(projects.traceableDatamodelConfigServiceApi)
  implementation(projects.traceableEdgeDecisionConverterUtils)
  implementation(projects.entityFetcherCache)

  implementation(localLibs.hypertrace.configservice.api)
  implementation(commonLibs.guice7)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.uuidcreator)
  implementation(commonLibs.re2j)
  implementation(commonLibs.commons.lang)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.traceable.platform.ipUtils)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.traceable.modsecurity.jni)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(commonLibs.traceable.traceenricher.constants)
  implementation(commonLibs.jackson.core)
  implementation(commonLibs.jackson.databind)
  implementation(commonLibs.commons.io)
  implementation(commonLibs.traceable.protection.engine.config.customsignature)
  implementation(commonLibs.traceable.protection.engine.processor.secrules)
  implementation(commonLibs.traceable.protection.engine.processor.conditionexpression)
  implementation(commonLibs.traceable.protection.engine.processing.common)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
