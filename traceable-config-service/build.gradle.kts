plugins {
  java
  application
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.docker-java-application-plugin") version "0.2.3"
  id("org.hypertrace.docker-publish-plugin") version "0.2.3"
  id("ai.traceable.docker-convention-plugin") version "1.2.1"
  id("org.hypertrace.integration-test-plugin") version "0.1.1"
}

dependencies {
  implementation(project(":sensitive-data-config-service-impl"))
  implementation("org.hypertrace.config.service:config-service-impl")

  implementation("org.hypertrace.core.grpcutils:grpc-server-utils:0.3.2")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.18")
  implementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.3.2")

  runtimeOnly("io.grpc:grpc-netty:1.33.1")
  implementation("com.typesafe:config:1.4.0")

  implementation("org.slf4j:slf4j-api:1.7.30")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")

  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
}

application {
  mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec>  {
  jvmArgs = listOf("-Dservice.name=${project.name}")
}

tasks.integrationTest {
  useJUnitPlatform()
}

// Customize integration test report to include source of service-impl
tasks.jacocoIntegrationTestReport {
  sourceSets(project(":sensitive-data-config-service-impl").sourceSets.getByName("main"))
}
