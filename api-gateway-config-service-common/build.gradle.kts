plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("ai.traceable.publish-plugin")
}

dependencies {
  api(projects.apiGatewayConfigServiceApi)

  implementation(libs.guice)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
}

tasks.test {
  useJUnitPlatform()
}
