plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("ai.traceable.publish-plugin")
}

dependencies {
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.entityservice.api)
  implementation(projects.configUtils)

  implementation(libs.traceable.platformGateway.eventInvalidationCache)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  implementation(libs.guava)
  implementation(libs.guice)
  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
}

tasks.test {
  useJUnitPlatform()
}
