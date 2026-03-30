plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.traceable.publish)
}

dependencies {
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.entityservice.api)
  implementation(commonLibs.hypertrace.entitychangeevent.api)
  implementation(commonLibs.hypertrace.framework.metrics.jakarta)
  implementation(commonLibs.hypertrace.kafkaStreams.eventListener)
  implementation(projects.configUtils)

  implementation(commonLibs.traceable.platform.eventInvalidationCache)
  implementation(commonLibs.traceable.protection.engine.processing.common)
  implementation(commonLibs.traceable.protection.engine.core)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  implementation(commonLibs.guava)
  implementation(commonLibs.guice7)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.slf4j2.api)

  testImplementation(commonLibs.bundles.junit.mockito)
}

tasks.test {
  useJUnitPlatform()
}
