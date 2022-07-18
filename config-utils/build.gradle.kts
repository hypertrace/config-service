plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("ai.traceable.publish-plugin")
}

dependencies {
  api(projects.traceableSpanProcessingConfigServiceApi)

  implementation(libs.protobuf.javautil)
  implementation(libs.uuidCreator)
  implementation(libs.slf4j.api)
  implementation(libs.re2j)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)

  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
