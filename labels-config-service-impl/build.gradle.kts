plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.labelsConfigServiceApi)
  implementation(projects.configObjectStore)
  implementation(projects.configProtoConverter)
  implementation(projects.configServiceChangeEventGenerator)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.guava)
  implementation(commonLibs.rholder.guava.retrying)
  implementation(commonLibs.rxjava3)

  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.grpcutils.rx.server)
  implementation(commonLibs.hypertrace.grpcutils.rx.client)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(testFixtures(projects.configServiceApi))
}

tasks.test {
  useJUnitPlatform()
}
