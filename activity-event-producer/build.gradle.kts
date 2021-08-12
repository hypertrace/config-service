plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)
  implementation(libs.traceable.activityevent.api)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.eventstore)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
}

tasks.test {
  useJUnitPlatform()
}
