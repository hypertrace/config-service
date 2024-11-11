plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.traceable.publish)
}

dependencies {
  compileOnly(commonLibs.lombok)
  compileOnly(commonLibs.slf4j2.api)

  annotationProcessor(commonLibs.lombok)

  implementation(projects.savedFilterConfigServiceApi)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.config.changeevent.api)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.hypertrace.kafkaStreams.eventListener)
  implementation(commonLibs.kafka.streams.protobuf.serde)
  implementation(commonLibs.commons.lang)
  implementation(commonLibs.guice)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.bundles.grpc.api)
  implementation(commonLibs.hypertrace.framework.metrics)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.junit)
}

tasks.test {
  useJUnitPlatform()
}
