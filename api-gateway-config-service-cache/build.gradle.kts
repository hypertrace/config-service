plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.traceable.publish)
}

dependencies {
  api(projects.apiGatewayConfigServiceApi)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  implementation(projects.apiGatewayConfigServiceCommon)

  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.changeeventapi)

  implementation(commonLibs.traceable.platform.eventInvalidationCache)

  implementation(commonLibs.guava)
  implementation(commonLibs.guice)
  implementation(commonLibs.typesafe.config)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
}

tasks.test {
  useJUnitPlatform()
}
