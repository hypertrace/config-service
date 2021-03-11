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
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("org.hypertrace.integration-test-plugin")
  id("org.hypertrace.jacoco-report-plugin")
  id("org.hypertrace.publish-plugin")
}

tasks.register<DockerCreateNetwork>("createIntegrationTestNetwork") {
  networkName.set("cfg-svc-int-test")
}

tasks.register<DockerRemoveNetwork>("removeIntegrationTestNetwork") {
  networkId.set("cfg-svc-int-test")
}

tasks.register<DockerPullImage>("pullMongoImage") {
  image.set("mongo:4.2.6")
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
  implementation(project(":config-service-impl"))
  implementation(project(":spaces-config-service-impl"))
  implementation("org.hypertrace.core.grpcutils:grpc-server-utils:0.3.3")
  implementation("org.hypertrace.core.serviceframework:platform-service-framework:0.1.21")
  implementation("com.typesafe:config:1.4.0")
  implementation("org.slf4j:slf4j-api:1.7.30")

  runtimeOnly("org.apache.logging.log4j:log4j-slf4j-impl:2.13.3")
  runtimeOnly("io.grpc:grpc-netty:1.36.0")

  //Integration test dependencies
  integrationTestImplementation("org.junit.jupiter:junit-jupiter:5.6.2")
  integrationTestImplementation("com.google.guava:guava:30.1-jre")
  integrationTestImplementation("org.yaml:snakeyaml:1.26")
  integrationTestImplementation(project(":config-service-impl"))
  integrationTestImplementation("org.hypertrace.core.serviceframework:integrationtest-service-framework:0.1.18")
  integrationTestImplementation("org.hypertrace.core.grpcutils:grpc-client-utils:0.3.3")
  constraints {
    runtimeOnly("io.netty:netty-codec-http2:4.1.60.Final") {
      because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-1083991")
    }
    runtimeOnly("io.netty:netty-handler-proxy:4.1.60.Final") {
      because("https://snyk.io/vuln/SNYK-JAVA-IONETTY-1070799")
    }
  }
}

application {
  mainClass.set("org.hypertrace.core.serviceframework.PlatformServiceLauncher")
}

// Config for gw run to be able to run this locally. Just execute gw run here on Intellij or on the console.
tasks.run<JavaExec> {
  jvmArgs = listOf("-Dservice.name=${project.name}")
}

tasks.jacocoIntegrationTestReport {
  sourceSets(project(":config-service-impl").sourceSets.getByName("main"))
}

hypertraceDocker {
  defaultImage {
    javaApplication {
      port.set(50101)
    }
  }
}