plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.hypertrace.publish)
}

dependencies {
  api(projects.configServiceApi)
  api(projects.configServiceChangeEventApi)
  api(commonLibs.hypertrace.grpcutils.context)

  implementation(projects.configServiceChangeEventGenerator)
  implementation(commonLibs.slf4j2.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(commonLibs.protobuf.javautil)

  testAnnotationProcessor(commonLibs.lombok)
  testCompileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
