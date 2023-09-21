plugins {
  `java-library`
}

dependencies {
  api(libs.grpc.api)
  api(libs.typesafe.config)

  implementation(projects.dashboardConfigServiceApi)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.guice)
  implementation(libs.protobuf.javautil)
  implementation(libs.slf4j.api)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(projects.configUtils)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
