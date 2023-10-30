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
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.hypertrace.docker.application)
  alias(commonLibs.plugins.hypertrace.docker.publish)
  alias(commonLibs.plugins.traceable.docker)
  alias(commonLibs.plugins.hypertrace.integrationtest)
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
  image.set(docker.registryCredentials.url.get() + "/traceable/mongo:4.4.22")
}

tasks.register<DockerPullImage>("pullEntityServiceImage") {
  image.set("hypertrace/entity-service:0.8.65")
}

tasks.register<DockerPullImage>("pullActorServiceImage") {
  image.set(docker.registryCredentials.url.get() + "/traceable/actor-service:${commonLibs.versions.traceable.actorservice.get()}")
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
  implementation(commonLibs.hypertrace.framework.grpc)

  runtimeOnly(commonLibs.grpc.netty)
  runtimeOnly(commonLibs.log4j.slf4j2.impl)
  runtimeOnly(platform(commonLibs.hypertrace.kafka.bom))
  runtimeOnly(commonLibs.kafka.avro.serializer)

  testFixturesImplementation(commonLibs.traceable.actorservice.api)
  testFixturesImplementation(commonLibs.traceable.insights.api)
  testFixturesImplementation(commonLibs.traceable.featureflag.api)
  testFixturesImplementation(commonLibs.traceable.licensemetering.api)
  testFixturesImplementation(commonLibs.hypertrace.grpcutils.context)
  testFixturesImplementation(commonLibs.hypertrace.grpcutils.client)

  // Integration test dependencies
  integrationTestImplementation(testFixtures(projects.traceableConfigService))
  integrationTestImplementation(commonLibs.traceable.actorservice.api)
  integrationTestImplementation(commonLibs.traceable.insights.api)
  integrationTestImplementation(commonLibs.traceable.featureflag.api)
  integrationTestImplementation(commonLibs.traceable.licensemetering.api)
  integrationTestImplementation(commonLibs.traceable.apinaming.model)
  integrationTestImplementation(commonLibs.traceable.platform.trainingEvaluationFramework)
  integrationTestImplementation(commonLibs.junit.jupiter)
  integrationTestImplementation(commonLibs.guava)
  integrationTestImplementation(commonLibs.hypertrace.integrationtest.framework)
  integrationTestImplementation(commonLibs.hypertrace.documentstore)
  integrationTestImplementation(commonLibs.hypertrace.grpcutils.client)
  integrationTestImplementation(commonLibs.hypertrace.grpcutils.context)
  integrationTestImplementation(commonLibs.hypertrace.entityservice.api)
  integrationTestImplementation(commonLibs.protobuf.javautil)
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
  integrationTestImplementation(projects.anomalyScoringConfigServiceApi)
  integrationTestImplementation(projects.regionConfigServiceApi)
  integrationTestImplementation(projects.riskConfigServiceApi)
  integrationTestImplementation(projects.sensitiveDataConfigServiceApi)
  integrationTestImplementation(projects.dataClassificationConfigServiceApi)
  integrationTestImplementation(projects.maliciousSourcesConfigServiceApi)
  integrationTestImplementation(projects.detectionExclusionConfigServiceApi)
  integrationTestImplementation(projects.splunkIntegrationConfigServiceApi)
  integrationTestImplementation(commonLibs.traceable.opadistributor.api)
  integrationTestImplementation(localLibs.hypertrace.configservice.partitioner.config.impl)
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
