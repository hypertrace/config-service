plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.externalUserAttributionConfigServiceApi)
  implementation(projects.userAttributionConfigServiceApi)
  implementation(libs.guice)
  implementation(libs.slf4j.api)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.uuidCreator)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
  testImplementation(libs.protobuf.javautil)

  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
