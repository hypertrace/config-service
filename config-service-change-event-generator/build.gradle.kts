plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.hypertrace.publish)
}

dependencies {
  api(projects.configServiceChangeEventApi)
  api(commonLibs.typesafe.config)
  api(commonLibs.hypertrace.grpcutils.context)

  implementation(projects.configProtoConverter)
  implementation(commonLibs.hypertrace.eventstore)
  implementation(commonLibs.guava)
  implementation(commonLibs.slf4j2.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testRuntimeOnly(commonLibs.log4j.slf4j2.impl)
}

tasks.test {
  useJUnitPlatform()
}
