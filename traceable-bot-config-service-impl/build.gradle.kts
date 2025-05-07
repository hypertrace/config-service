plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
}

dependencies {
  implementation(projects.configUtils)
  implementation(projects.traceableBotConfigServiceApi)
  implementation(projects.entityFetcherCache)

  implementation(commonLibs.grpc.api)
  implementation(commonLibs.typesafe.config)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)

  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)

  implementation(localLibs.hypertrace.configservice.validation)
  implementation(localLibs.hypertrace.configservice.api)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.protobuf.javautil)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()

  // Make sure config files are copied before tests run
  dependsOn("copyConfigFiles")

  // Ensure cleanup happens after tests
  finalizedBy("cleanupTestConfig")
}

// Task to copy configuration files from traceable-config-service to test resources
// This will only be executed when the test task runs due to the dependsOn relationship
tasks.register<Copy>("copyConfigFiles") {
  from("${rootProject.projectDir}/traceable-config-service/src/main/resources/configs/common/application.conf")
  into(layout.buildDirectory.dir("resources/test"))
  // Ensure the directory exists
  doFirst {
    layout.buildDirectory.dir("resources/test").get().asFile.mkdirs()
  }
}

// Task to remove the config file after tests are done
tasks.register<Delete>("cleanupTestConfig") {
  delete(layout.buildDirectory.file("resources/test/application.conf"))
}

// Explicitly exclude application.conf coped from traceable config service from the jar task
tasks.jar {
  exclude("**/application.conf")
}
