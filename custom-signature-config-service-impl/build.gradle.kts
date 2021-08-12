plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.activityEventProducer)
  api(libs.grpc.api)
  api(libs.typesafe.config)
  implementation(projects.customSignatureConfigServiceApi)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.guice)
  implementation(libs.guava)
  implementation(libs.protobuf.javautil)
  implementation(libs.slf4j.api)
  implementation(libs.uuidCreator)
  implementation(libs.re2j)

  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.traceable.activityevent.api)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
