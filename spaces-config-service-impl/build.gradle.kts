plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.spacesConfigServiceApi)
  implementation(projects.configServiceApi)
  implementation(projects.configProtoConverter)
  implementation(libs.guava)
  implementation(libs.rxjava3)

  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.grpcutils.rxserver)
  implementation(libs.hypertrace.grpcutils.rxclient)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
  testImplementation(testFixtures(projects.configServiceApi))
}

tasks.test {
  useJUnitPlatform()
}
