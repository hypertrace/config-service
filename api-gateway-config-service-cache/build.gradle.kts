plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("ai.traceable.publish-plugin")
}

dependencies {
  api(projects.apiGatewayConfigServiceApi)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  implementation(projects.apiGatewayConfigServiceCommon)

  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.configservice.changeeventapi)

  implementation(libs.traceable.platformGateway.eventInvalidationCache)

  implementation(libs.guava)
  implementation(libs.guice)
  implementation(libs.typesafe.config)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
}

tasks.test {
  useJUnitPlatform()
}
