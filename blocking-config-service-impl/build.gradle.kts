plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.anomalyConfigServiceApi)
  api(projects.blockingConfigServiceApi)
  api(projects.customSignatureConfigServiceApi)
  api(projects.regionConfigServiceApi)
  api(projects.iprangeConfigServiceApi)
  api(projects.rateLimitingConfigServiceApi)
  api(projects.maliciousSourcesConfigServiceApi)
  api(projects.featureCachingClient)

  implementation(libs.traceable.opaDistributor.api)
  implementation(libs.traceable.actorService.api)
  implementation(libs.traceable.platformGateway.ipUtils)
  implementation(libs.hypertrace.configservice.protoconverter)
  implementation(projects.configUtils)

  implementation(libs.guice)
  implementation(libs.guava)
  implementation(libs.protobuf.javautil)
  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)
  implementation(libs.commons.csv)

  implementation(libs.hypertrace.grpcutils.context)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.entityservice.api)
  implementation(libs.hypertrace.framework.metrics)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.inline)
  testImplementation(libs.grpc.core)
  testAnnotationProcessor(libs.lombok)
  testCompileOnly(libs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
