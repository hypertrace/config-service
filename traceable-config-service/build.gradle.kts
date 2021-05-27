import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStartContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStopContainer
import com.bmuschko.gradle.docker.tasks.image.DockerPullImage
import com.bmuschko.gradle.docker.tasks.network.DockerCreateNetwork
import com.bmuschko.gradle.docker.tasks.network.DockerRemoveNetwork

plugins {
  java
  application
  jacoco
  `java-test-fixtures`
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.docker-java-application-plugin") version "0.8.2"
  id("org.hypertrace.docker-publish-plugin") version "0.8.2"
  id("ai.traceable.docker-convention-plugin") version "1.2.2"
  id("org.hypertrace.integration-test-plugin") version "0.1.3"
}

tasks.register<DockerCreateNetwork>("createIntegrationTestNetwork") {
  networkName.set("traceable-cfg-svc-int-test")
}

tasks.register<DockerRemoveNetwork>("removeIntegrationTestNetwork") {
  networkId.set("traceable-cfg-svc-int-test")
}

tasks.register<DockerPullImage>("pullMongoImage") {
  image.set(docker.registryCredentials.url.get() + "/mongo:4.2.7")
}

tasks.register<DockerCreateContainer>("createMongoContainer") {
  dependsOn("createIntegrationTestNetwork")
  dependsOn("pullMongoImage")
  targetImageId(tasks.getByName<DockerPullImage>("pullMongoImage").image)
  containerName.set("mongo-local")
  hostConfig.network.set(tasks.getByName<DockerCreateNetwork>("createIntegrationTestNetwork").networkId)
  hostConfig.portBindings.set(listOf("37017:27017"))
  hostConfig.autoRemove.set(true)
}

tasks.register<DockerStartContainer>("startMongoContainer") {
  dependsOn("createMongoContainer")
  targetContainerId(tasks.getByName<DockerCreateContainer>("createMongoContainer").containerId)
}

tasks.register<DockerStopContainer>("stopMongoContainer") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createMongoContainer").containerId)
  finalizedBy("removeIntegrationTestNetwork")
}

tasks.integrationTest {
  useJUnitPlatform()
  dependsOn("startMongoContainer")
  finalizedBy("stopMongoContainer")
}

dependencies {
  implementation(project(":activity-event-producer"))
  implementation(project(":sensitive-data-config-service-impl"))
  implementation(project(":rate-limiting-config-service-impl"))
  implementation(project(":local-processing-config-service-impl"))
  implementation(project(":region-config-service-impl"))
  implementation(project(":custom-signature-config-service-impl"))
  implementation(project(":blocking-config-service-impl"))
  implementation(project(":user-attribution-config-service-impl"))
  implementation(project(":external-user-attribution-config-service-impl"))
  implementation(project(":threat-management-config-service-impl"))
  implementation(project(":anomaly-config-service-impl"))
  implementation("org.hypertrace.config.service:config-service")
  implementation("org.hypertrace.config.service:config-service-impl")
  implementation("org.hypertrace.core.grpcutils:grpc-server-utils:0.4.1")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.20")
  implementation("ai.traceable.activity.event.service:activity-event-api:0.3.0")
  implementation("com.typesafe:config:1.4.1")
  implementation("org.slf4j:slf4j-api:1.7.30")
  implementation("io.confluent:kafka-avro-serializer:6.0.1")
  runtimeOnly("io.grpc:grpc-netty:1.37.0")
  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.14.0")

  // Integration test dependencies
  integrationTestImplementation(testFixtures(project(":traceable-config-service")))
  integrationTestImplementation("ai.traceable.platform:insights-service-api:0.29.5")
  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.7.1")
  integrationTestImplementation("com.google.guava:guava:30.1.1-jre")
  integrationTestImplementation("org.hypertrace.core.serviceframework:integrationtest-service-framework:0.1.20")
  integrationTestImplementation("org.hypertrace.core.documentstore:document-store:0.5.0")
  integrationTestImplementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.4.1")
  integrationTestImplementation("com.google.protobuf:protobuf-java-util:3.15.8")

  testFixturesImplementation("ai.traceable.platform:insights-service-api:0.29.5")
}

application {
  mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dservice.name=${project.name}")
}

// Customize integration test report to include source of service-impl
tasks.jacocoIntegrationTestReport {
  sourceSets(project(":sensitive-data-config-service-impl").sourceSets.getByName("main"))
}

hypertraceDocker {
  defaultImage {
    javaApplication {
      ports.addAll(50101, 50102)
      adminPort.set(50103)
    }
  }
}
