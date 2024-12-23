plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(commonLibs.typesafe.config)
  api(commonLibs.grpc.api)
  implementation(projects.maliciousSourcesConfigServiceApi)
  implementation(projects.configUtils)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.traceable.platform.ipUtils)

  implementation(commonLibs.guice7)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.hypertrace.grpcutils.client)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)
  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(commonLibs.mockito.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
