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
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("ai.traceable.docker-convention-plugin")
  id("org.hypertrace.integration-test-plugin") version "0.2.0"
}

tasks.register<DockerCreateNetwork>("createIntegrationTestNetwork") {
  mustRunAfter("compileIntegrationTestJava")
  networkId.set("traceable-cfg-svc-int-test")
  networkName.set("traceable-cfg-svc-int-test")
}

tasks.register<DockerRemoveNetwork>("removeIntegrationTestNetwork") {
  networkId.set("traceable-cfg-svc-int-test")
}

tasks.register<DockerPullImage>("pullMongoImage") {
  image.set(docker.registryCredentials.url.get() + "/mongo:4.4.0")
}

tasks.register<DockerPullImage>("pullEntityServiceImage") {
  image.set(docker.registryCredentials.url.get() + "/hypertrace/entity-service:0.8.37")
}

tasks.register<DockerPullImage>("pullActorServiceImage") {
  image.set(docker.registryCredentials.url.get() + "/traceable/actor-service:0.2.81")
}

tasks.register<DockerStartContainer>("startMongoContainer") {
  dependsOn("createMongoContainer")
  targetContainerId(tasks.getByName<DockerCreateContainer>("createMongoContainer").containerId)
}

tasks.register<DockerStartContainer>("startEntityServiceContainer") {
  dependsOn("startMongoContainer")
  dependsOn("createEntityServiceContainer")
  targetContainerId(tasks.getByName<DockerCreateContainer>("createEntityServiceContainer").containerId)
}

tasks.register<DockerStartContainer>("startActorServiceContainer") {
  dependsOn("startEntityServiceContainer")
  dependsOn("createActorServiceContainer")
  targetContainerId(tasks.getByName<DockerCreateContainer>("createActorServiceContainer").containerId)
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

tasks.register<DockerCreateContainer>("createEntityServiceContainer") {
  dependsOn("createIntegrationTestNetwork")
  dependsOn("pullEntityServiceImage")
  targetImageId(tasks.getByName<DockerPullImage>("pullEntityServiceImage").image)
  containerName.set("entity-service-local")
  envVars.put("mongo_host", tasks.getByName<DockerCreateContainer>("createMongoContainer").containerName)
  hostConfig.portBindings.set(listOf("60061:50061"))
  hostConfig.binds.put("$projectDir/src/integrationTest/resources/config-entity-service-test/application.conf", "/app/resources/configs/entity-service/application.conf")
  hostConfig.network.set(tasks.getByName<DockerCreateNetwork>("createIntegrationTestNetwork").networkId)
  hostConfig.autoRemove.set(true)
}

tasks.register<DockerCreateContainer>("createActorServiceContainer") {
  dependsOn("createIntegrationTestNetwork")
  dependsOn("pullActorServiceImage")
  targetImageId(tasks.getByName<DockerPullImage>("pullActorServiceImage").image)
  containerName.set("actor-service-local")
  envVars.put("entity_service_host", tasks.getByName<DockerCreateContainer>("createEntityServiceContainer").containerName)
  exposePorts("tcp", listOf(50888))
  hostConfig.portBindings.set(listOf("60888:50888"))
  hostConfig.binds.put("$projectDir/src/integrationTest/resources/config-actor-service-test/application.conf", "/app/resources/configs/actor-service/application.conf")
  hostConfig.network.set(tasks.getByName<DockerCreateNetwork>("createIntegrationTestNetwork").networkId)
  hostConfig.autoRemove.set(true)
}

tasks.register<DockerStopContainer>("stopMongoContainer") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createMongoContainer").containerId)
  finalizedBy("removeIntegrationTestNetwork")
}

tasks.register<DockerStopContainer>("stopEntityServiceContainer") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createEntityServiceContainer").containerId)
  finalizedBy("stopMongoContainer")
}

tasks.register<DockerStopContainer>("stopActorServiceContainer") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createActorServiceContainer").containerId)
  finalizedBy("stopEntityServiceContainer")
}

tasks.integrationTest {
  useJUnitPlatform()
  dependsOn("startActorServiceContainer")
  finalizedBy("stopActorServiceContainer")
  maxHeapSize = "1024m"
}

dependencies {
  implementation(projects.traceableConfigServiceFactory)
  implementation(libs.hypertrace.grpc.framework)

  runtimeOnly(libs.grpc.netty)
  runtimeOnly(libs.slf4j.log4jimpl)
  runtimeOnly(libs.kafka.avro.serializer)

  constraints {
    runtimeOnly(libs.jersey.common)
  }

  testFixturesImplementation(libs.traceable.actorService.api)
  testFixturesImplementation(libs.traceable.insights.api)
  testFixturesImplementation(libs.traceable.featureFlag.api)
  testFixturesImplementation(libs.traceable.licensemetering.api)
  testFixturesImplementation(libs.hypertrace.grpcutils.context)
  testFixturesImplementation(libs.hypertrace.grpcutils.client)

  // Integration test dependencies
  integrationTestImplementation(testFixtures(projects.traceableConfigService))
  integrationTestImplementation(libs.traceable.actorService.api)
  integrationTestImplementation(libs.traceable.insights.api)
  integrationTestImplementation(libs.traceable.featureFlag.api)
  integrationTestImplementation(libs.traceable.licensemetering.api)
  integrationTestImplementation(libs.traceable.apiNamingModel)
  integrationTestImplementation(libs.traceable.platformGateway.trainingEvaluationFramework)
  integrationTestImplementation(libs.junit.jupiter)
  integrationTestImplementation(libs.guava)
  integrationTestImplementation(libs.hypertrace.framework.integrationtest)
  integrationTestImplementation(libs.hypertrace.documentstore)
  integrationTestImplementation(libs.hypertrace.grpcutils.client)
  integrationTestImplementation(libs.hypertrace.grpcutils.context)
  integrationTestImplementation(libs.hypertrace.entityservice.api)
  integrationTestImplementation(libs.hypertrace.entityservice.client)
  integrationTestImplementation(libs.protobuf.javautil)
  integrationTestImplementation(projects.iprangeConfigServiceApi)
  integrationTestImplementation(projects.apiAttributeOverrideServiceApi)
  integrationTestImplementation(projects.blockingConfigServiceApi)
  integrationTestImplementation(projects.blockingConfigServiceImpl)
  integrationTestImplementation(projects.customSignatureConfigServiceApi)
  integrationTestImplementation(projects.localProcessingConfigServiceApi)
  integrationTestImplementation(projects.localProcessingConfigServiceImpl)
  integrationTestImplementation(projects.anomalyConfigServiceRegistry)
  integrationTestImplementation(projects.anomalyConfigServiceUtils)
  integrationTestImplementation(projects.rateLimitingConfigServiceApi)
  integrationTestImplementation(projects.regionConfigServiceApi)
  integrationTestImplementation(projects.riskConfigServiceApi)
  integrationTestImplementation(projects.sensitiveDataConfigServiceApi)
  integrationTestImplementation(projects.dataClassificationConfigServiceApi)
  integrationTestImplementation(projects.maliciousSourcesConfigServiceApi)
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
