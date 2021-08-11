plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.configServiceApi)

  implementation(libs.jackson.databind)
  implementation(libs.guava)
  implementation(libs.protobuf.javautil)
  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)

  implementation(libs.hypertrace.documentstore)
  implementation(libs.hypertrace.grpcutils.context)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.hypertrace.grpcutils.client)
}

tasks.test {
  useJUnitPlatform()
}
