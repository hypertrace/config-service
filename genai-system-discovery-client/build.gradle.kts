plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.traceable.publish)
}

dependencies {

  api(projects.genaiSystemDiscoveryConfigServiceApi)

  implementation(commonLibs.grpc.api)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.hypertrace.framework.metrics.jakarta)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.kafkaStreams.eventListener)
  implementation(localLibs.hypertrace.configservice.changeeventapi)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(testFixtures(commonLibs.hypertrace.kafkaStreams.eventListener))
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
