plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.spacesConfigServiceApi)
  implementation(projects.configServiceApi)
  implementation(projects.configProtoConverter)
  implementation(commonLibs.guava)
  implementation(commonLibs.rxjava3)

  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.grpcutils.rx.server)
  implementation(commonLibs.hypertrace.grpcutils.rx.client)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(testFixtures(projects.configServiceApi))
}

tasks.test {
  useJUnitPlatform()
}
