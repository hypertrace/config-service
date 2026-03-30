plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.externalUserAttributionConfigServiceApi)
  api(commonLibs.typesafe.config)
  implementation(projects.userAttributionConfigServiceApi)
  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.uuidcreator)
  implementation(commonLibs.protobuf.javautil)
  implementation(localLibs.hypertrace.configservice.objectstore)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(commonLibs.protobuf.javautil)
  testImplementation(commonLibs.grpc.core)

  testAnnotationProcessor(commonLibs.lombok)
  testCompileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
