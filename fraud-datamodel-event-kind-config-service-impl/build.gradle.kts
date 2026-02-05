plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  implementation(projects.fraudDatamodelEventKindConfigServiceApi)
  implementation(commonLibs.protobuf.java)
  implementation(commonLibs.protobuf.javautil)

  implementation(commonLibs.jackson.yaml)
  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
}

tasks.test {
  useJUnitPlatform()
}
