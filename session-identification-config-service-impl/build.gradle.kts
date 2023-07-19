plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(libs.typesafe.config)
  api(libs.grpc.api)
  api(libs.hypertrace.configservice.changeeventgenerator)
  api(projects.featureCachingClient)
  implementation(projects.sensitiveDataConfigServiceApi)
  implementation(projects.configUtils)
  implementation(projects.externalAgentAttributeConfigServiceApi)
  implementation(projects.sessionIdentificationConfigServiceApi)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.configservice.protoconverter)

  implementation(libs.guice)
  implementation(libs.protobuf.javautil)
  implementation(libs.slf4j.api)
  implementation(libs.uuidCreator)
  implementation(libs.hypertrace.grpcutils.client)

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
