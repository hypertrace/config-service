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
  mustRunAfter("compileIntegrationTestJava")
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
  implementation(projects.activityEventProducer)
  implementation(projects.sensitiveDataConfigServiceImpl)
  implementation(projects.rateLimitingConfigServiceImpl)
  implementation(projects.licenseStatusConfigServiceImpl)
  implementation(projects.localProcessingConfigServiceImpl)
  implementation(projects.regionConfigServiceImpl)
  implementation(projects.iprangeConfigServiceImpl)
  implementation(projects.customSignatureConfigServiceImpl)
  implementation(projects.blockingConfigServiceImpl)
  implementation(projects.userAttributionConfigServiceImpl)
  implementation(projects.externalUserAttributionConfigServiceImpl)
  implementation(projects.threatManagementConfigServiceImpl)
  implementation(projects.anomalyConfigServiceImpl)
  implementation(projects.anomalyConfigServiceUtils)
  implementation(projects.riskConfigServiceImpl)
  implementation(projects.alertingConfigServiceImpl)
  implementation(projects.reportingConfigServiceImpl)
  implementation(projects.dataClassificationConfigServiceImpl)
  implementation(libs.hypertrace.configservice.server)
  implementation(libs.hypertrace.configservice.impl)
  implementation(libs.hypertrace.grpcutils.server)
  implementation(libs.hypertrace.grpcutils.client)
  implementation(libs.hypertrace.framework.container)
  implementation(libs.hypertrace.framework.metrics)
  implementation(libs.hypertrace.configservice.notification.rule.impl)
  implementation(libs.hypertrace.configservice.notification.channel.impl)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(libs.traceable.activityevent.api)
  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)
  implementation(libs.kafka.avro.serializer)
  runtimeOnly(libs.grpc.netty)
  runtimeOnly(libs.slf4j.log4jimpl)

  testFixturesImplementation(libs.traceable.insights.api)
  testFixturesImplementation(libs.traceable.featureFlag.futureClient)
  testFixturesImplementation(libs.traceable.licensemetering.api)
  testFixturesImplementation(libs.hypertrace.grpcutils.context)
  testFixturesImplementation(libs.hypertrace.grpcutils.client)

  // Integration test dependencies
  integrationTestImplementation(testFixtures(projects.traceableConfigService))
  integrationTestImplementation(libs.traceable.insights.api)
  integrationTestImplementation(libs.traceable.featureFlag.futureClient)
  integrationTestImplementation(libs.traceable.licensemetering.api)
  integrationTestImplementation(libs.junit.jupiter)
  integrationTestImplementation(libs.guava)
  integrationTestImplementation(libs.hypertrace.framework.integrationtest)
  integrationTestImplementation(libs.hypertrace.documentstore)
  integrationTestImplementation(libs.hypertrace.grpcutils.client)
  integrationTestImplementation(libs.hypertrace.grpcutils.context)
  integrationTestImplementation(libs.protobuf.javautil)
  integrationTestImplementation(projects.iprangeConfigServiceApi)
  integrationTestImplementation(projects.riskConfigServiceApi)
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
