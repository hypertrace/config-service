plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  api(commonLibs.bundles.grpc.api)
  api(commonLibs.hypertrace.framework.metrics)
  api(commonLibs.commons.lang)
  api(projects.configServiceCommons)

  implementation(localLibs.hypertrace.configservice.objectstore)

  testImplementation(commonLibs.bundles.junit.mockito)
}

tasks.test {
  useJUnitPlatform()
}
