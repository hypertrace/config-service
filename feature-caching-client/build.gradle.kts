plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(libs.guice)
  api(libs.typesafe.config)

  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.traceable.featureFlag.api)
  implementation(libs.slf4j.api)
  implementation(libs.guava)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
