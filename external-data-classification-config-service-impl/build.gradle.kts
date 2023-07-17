plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(libs.grpc.api)
  api(libs.typesafe.config)
  api(projects.externalDataClassificationConfigServiceApi)
  api(projects.featureCachingClient)

  implementation(projects.dataClassificationConfigServiceApi)
  implementation(projects.sensitiveDataConfigServiceApi)
  implementation(projects.sessionIdentificationConfigServiceApi)
  implementation(libs.traceable.insights.api)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.slf4j.api)
  implementation(libs.guice)
  implementation(projects.configUtils)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.protobuf.javautil)
  testImplementation(libs.mockito.junit)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
