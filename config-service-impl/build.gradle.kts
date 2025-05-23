plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.configServiceApi)

  implementation(projects.configServiceChangeEventGenerator)
  implementation(projects.configProtoConverter)

  implementation(commonLibs.jackson.databind)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.slf4j2.api)

  implementation(commonLibs.hypertrace.documentstore)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.framework.documentstore.metrics.jakarta)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.hypertrace.grpcutils.client)
}

tasks.test {
  useJUnitPlatform()
}
