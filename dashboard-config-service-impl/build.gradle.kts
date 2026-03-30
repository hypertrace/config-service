plugins {
  `java-library`
}

dependencies {
  api(commonLibs.grpc.api)
  api(commonLibs.typesafe.config)

  implementation(projects.dashboardConfigServiceApi)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(commonLibs.guice7)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(projects.configUtils)
  implementation(commonLibs.traceable.notification.messageApi)
  implementation(commonLibs.hypertrace.eventstore)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
