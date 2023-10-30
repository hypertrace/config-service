plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  api(projects.astScanProfileConfigServiceApi)
  implementation(projects.configUtils)
  implementation(commonLibs.guice)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
