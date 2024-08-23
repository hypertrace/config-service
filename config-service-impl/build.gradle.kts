plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.configServiceApi)

  implementation(projects.configServiceChangeEventGenerator)

  implementation(commonLibs.jackson.databind)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.slf4j2.api)

  implementation("org.hypertrace.core.documentstore:document-store:0.7.66-SNAPSHOT")
  implementation(commonLibs.hypertrace.grpcutils.context)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.hypertrace.grpcutils.client)
}

tasks.test {
  useJUnitPlatform()
}
