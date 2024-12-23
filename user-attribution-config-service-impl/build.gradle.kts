plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.userAttributionConfigServiceApi)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(projects.configUtils)
  implementation(projects.externalAgentAttributeConfigServiceApi)
  implementation(projects.featureCachingClient)
  implementation(commonLibs.guice7)
  implementation(commonLibs.guava)
  implementation(commonLibs.re2j)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(commonLibs.jackson.yaml)
  implementation(commonLibs.re2j)
  implementation(commonLibs.protobuf.javautil)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
  testAnnotationProcessor(commonLibs.lombok)
  testCompileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
