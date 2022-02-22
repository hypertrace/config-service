plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.localProcessingConfigServiceApi)
  implementation(projects.anomalyConfigServiceApi)
  implementation(projects.customSignatureConfigServiceApi)
  implementation(libs.hypertrace.configservice.api)
  implementation(libs.hypertrace.configservice.objectstore)
  implementation(libs.guice)
  implementation(libs.guava)
  implementation(libs.protobuf.javautil)
  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)
  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(libs.uuidCreator)
  implementation(libs.hypertrace.entityservice.api)
  implementation(libs.traceable.apiNamingModel)
  implementation(libs.traceable.platformGateway.deepDataStore)
  implementation(libs.traceable.platformGateway.trainingEvaluationFramework)
  implementation(libs.rholder.guava.retrying)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.inline)
  testImplementation(testFixtures(libs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
