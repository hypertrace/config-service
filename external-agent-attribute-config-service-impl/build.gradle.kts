plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.externalAgentAttributeConfigServiceApi)
  api(libs.typesafe.config)
  api(projects.featureCachingClient)
  implementation(projects.userAttributionConfigServiceApi)
  implementation(libs.guice)
  implementation(libs.slf4j.api)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.uuidCreator)
  implementation(libs.protobuf.javautil)
  implementation(projects.configUtils)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
  testImplementation(libs.protobuf.javautil)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
