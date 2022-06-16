plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.apiAttributeOverrideServiceApi)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.validation)
  implementation(libs.hypertrace.configservice.changeeventgenerator)

  implementation(libs.guice)
  implementation(libs.guava)
  implementation(libs.protobuf.javautil)
  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)

  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.grpc.core)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
