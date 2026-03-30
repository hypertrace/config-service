plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.traceable.publish)
}

dependencies {
  compileOnly(commonLibs.slf4j2.api)

  compileOnly(commonLibs.lombok)
  annotationProcessor(commonLibs.lombok)

  api(projects.savedFilterConfigServiceApi)

  api(commonLibs.hypertrace.config.changeevent.api)
  api(commonLibs.hypertrace.kafkaStreams.eventListener)

  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.kafka.streams.protobuf.serde)
  implementation(commonLibs.commons.lang)
  implementation(commonLibs.guice7)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.bundles.grpc.api)
  implementation(commonLibs.hypertrace.framework.metrics.jakarta)

  testImplementation(commonLibs.bundles.junit.mockito)
}

tasks.test {
  useJUnitPlatform()
}
