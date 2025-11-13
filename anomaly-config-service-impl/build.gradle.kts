plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.traceable.publish)
}

dependencies {
  api(projects.anomalyConfigServiceApi)
  api(projects.anomalyConfigServiceRegistry)
  api(projects.featureCachingClient)
  implementation(projects.configUtils)
  implementation(projects.entityFetcherCache)
  implementation(projects.modsecurityUtils)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)

  implementation(commonLibs.guice7)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.uuidcreator)

  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.framework.metrics.jakarta)
  // https://traceableai.atlassian.net/browse/ENG-10685
  // anomaly-config-service should be carved out soon to avoid chances of dependency loop..
  implementation(commonLibs.traceable.licensemetering.api)
  implementation(commonLibs.traceable.protection.rules.webapp)
  implementation(commonLibs.traceable.protection.rules.apiprotect)
  implementation(commonLibs.traceable.protection.rules.aiapp)
  implementation(commonLibs.traceable.protection.engine.config.webapp)
  implementation(commonLibs.traceable.protection.engine.config.apiprotect)
  implementation(commonLibs.traceable.protection.engine.processor.secrules)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.grpc.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
  testAnnotationProcessor(commonLibs.lombok)
  testCompileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
