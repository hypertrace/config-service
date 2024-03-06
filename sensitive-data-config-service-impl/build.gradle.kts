plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.sensitiveDataConfigServiceApi)
  api(projects.featureCachingClient)
  api(localLibs.hypertrace.configservice.changeeventgenerator)
  api(commonLibs.hypertrace.grpcutils.client)
  api(commonLibs.typesafe.config)
  api(commonLibs.grpc.api)
  implementation(projects.configUtils)
  implementation(projects.dataClassificationConfigServiceApi)

  implementation(localLibs.hypertrace.configservice.api)
  implementation(commonLibs.traceable.insights.api)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.re2j)
  implementation(commonLibs.guice)
  implementation(commonLibs.uuidcreator)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(commonLibs.mockito.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
  testImplementation(testFixtures(projects.traceableConfigService))
  testAnnotationProcessor(commonLibs.lombok)
  testCompileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
