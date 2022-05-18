plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.sensitiveDataConfigServiceApi)
  api(projects.featureCachingClient)
  api(libs.hypertrace.configservice.changeeventgenerator)
  api(libs.hypertrace.grpcutils.client)
  api(libs.typesafe.config)
  api(libs.grpc.api)
  implementation(projects.configUtils)
  implementation(projects.dataClassificationConfigServiceApi)

  implementation(libs.hypertrace.configservice.api)
  implementation(libs.traceable.insights.api)
  implementation(libs.guava)
  implementation(libs.protobuf.javautil)
  implementation(libs.slf4j.api)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.re2j)
  implementation(libs.guice)
  implementation(libs.uuidCreator)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
  testImplementation(testFixtures(projects.traceableConfigService))
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
