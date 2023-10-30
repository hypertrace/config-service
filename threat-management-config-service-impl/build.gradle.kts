plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.threatManagementConfigServiceApi)
  implementation(projects.configUtils)
  implementation(localLibs.hypertrace.configservice.api)

  implementation(commonLibs.guice)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.typesafe.config)

  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(commonLibs.grpc.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
  testAnnotationProcessor(commonLibs.lombok)
  testCompileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
