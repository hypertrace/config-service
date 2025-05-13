plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(commonLibs.typesafe.config)
  api(commonLibs.grpc.api)
  implementation(projects.certificateManagementConfigServiceApi)
  implementation(projects.configUtils)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(commonLibs.guice7)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.uuidcreator)
  implementation(commonLibs.guava)

  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
