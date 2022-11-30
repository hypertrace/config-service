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
  implementation(libs.commons.net)
  implementation(libs.commons.validator)
  implementation(libs.typesafe.config)
  implementation(libs.commons.csv)

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
