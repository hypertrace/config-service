plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.licenseStatusConfigServiceApi)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(commonLibs.guice)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  // https://traceableai.atlassian.net/browse/ENG-20659
  // This is temporary. Remove this once license enforcer changes are in place
  implementation(commonLibs.traceable.licensemetering.api)
  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)
  implementation(commonLibs.hypertrace.framework.metrics)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
