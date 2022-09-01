plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.activityEventProducer)
  api(libs.typesafe.config)
  api(libs.grpc.api)
  implementation(projects.iprangeConfigServiceApi)
  implementation(projects.configUtils)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(libs.guice)
  implementation(libs.protobuf.javautil)
  implementation(libs.slf4j.api)
  implementation(libs.uuidCreator)
  implementation(libs.guava)

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
