plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.grpc.api)
  implementation(commonLibs.protobuf.javautil)

  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.jackson.databind)
  implementation(commonLibs.jackson.yaml)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(projects.traceableEdgeDecisionConfigServiceApi)
}

tasks.test {
  useJUnitPlatform()
}
