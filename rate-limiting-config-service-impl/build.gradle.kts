plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.rateLimitingConfigServiceApi)
  api(libs.hypertrace.configservice.api)

  implementation(projects.activityEventProducer)
  implementation(projects.configUtils)

  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)
  implementation(libs.guice)
  implementation(libs.re2j)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(libs.traceable.platformGateway.ipUtils)

  implementation(libs.traceable.activityevent.api)
  implementation(libs.protobuf.javautil)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.hypertrace.grpcutils.client)
  testImplementation(libs.commons.lang)
  testImplementation(libs.grpc.core)
  testImplementation(libs.hypertrace.grpcutils.client)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
