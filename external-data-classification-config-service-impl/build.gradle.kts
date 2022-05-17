plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(libs.guice)
  api(libs.grpc.api)
  api(libs.typesafe.config)
  api(projects.externalDataClassificationConfigServiceApi)

  implementation(projects.dataClassificationConfigServiceApi)
  implementation(projects.sensitiveDataConfigServiceApi)
  implementation(libs.traceable.insights.api)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.slf4j.api)
  implementation(libs.protobuf.javautil)
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
