plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.fraudDatamodelEventKindConfigServiceApi)
  api(commonLibs.grpc.api)

  implementation(commonLibs.grpc.stub)
  implementation(commonLibs.protobuf.java)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.hypertrace.grpcutils.context)

  implementation(commonLibs.jackson.databind)
  implementation(commonLibs.jackson.yaml)
  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.bundles.junit.mockito)
}

tasks.test {
  useJUnitPlatform()
}
