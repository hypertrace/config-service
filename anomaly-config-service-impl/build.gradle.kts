plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.anomalyConfigServiceApi)
  api(projects.anomalyConfigServiceRegistry)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.changeeventgenerator)

  implementation(libs.guice)
  implementation(libs.guava)
  implementation(libs.protobuf.javautil)
  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)
  implementation(libs.uuidCreator)

  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  // https://traceableai.atlassian.net/browse/ENG-10685
  // anomaly-config-service should be carved out soon to avoid chances of dependency loop..
  implementation(libs.traceable.licensemetering.api)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.grpc.core)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
