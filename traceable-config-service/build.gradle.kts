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
  image.set(docker.registryCredentials.url.get() + "/traceable/mongodb-curl:8.0.4")
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

tasks.register<com.bmuschko.gradle.docker.tasks.image.DockerPullImage>("pullCorazaImage") {
  image.set(docker.registryCredentials.url.get() + "/traceable/coraza-waf-service:${commonLibs.versions.traceable.corazaWafService.get()}")
}

val corazaContainer = "coraza-local"
tasks.register<DockerCreateContainer>("createCorazaContainer") {
  dependsOn("createIntegrationTestNetwork", "pullCorazaImage")
  containerName.set(corazaContainer)

  targetImageId(tasks.named<com.bmuschko.gradle.docker.tasks.image.DockerPullImage>("pullCorazaImage").get().image)
  hostConfig.apply {
    network.set(tasks.getByName<DockerCreateNetwork>("createIntegrationTestNetwork").networkId)
    portBindings.set(listOf("9000:9000"))
    autoRemove.set(true)
    binds.put("$projectDir/conf-files", "/app/conf")
  }
}

tasks.register<DockerStartContainer>("startCorazaContainer") {
  dependsOn("createCorazaContainer")
  targetContainerId(tasks.named<DockerCreateContainer>("createCorazaContainer").get().containerId)
}

tasks.register<DockerStopContainer>("stopCorazaContainer") {
  targetContainerId(tasks.named<DockerCreateContainer>("createCorazaContainer").get().containerId)
  finalizedBy("removeIntegrationTestNetwork")
}

tasks.register<DockerStopContainer>("stopMongoContainer") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createMongoContainer").containerId)
  finalizedBy("stopCorazaContainer")
}

tasks.register<DockerStopContainer>("stopEntityServiceContainer") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createEntityServiceContainer").containerId)
  finalizedBy("stopMongoContainer")
}

tasks.register<DockerStopContainer>("stopAllContainers") {
  targetContainerId(tasks.getByName<DockerCreateContainer>("createActorServiceContainer").containerId)
  finalizedBy("stopEntityServiceContainer")
}

tasks.integrationTest {
  useJUnitPlatform()
  dependsOn("startActorServiceContainer")
  dependsOn("startCorazaContainer")
  finalizedBy("stopAllContainers")
  maxHeapSize = "1024m"
}

dependencies {
  implementation(projects.traceableConfigServiceFactory)
  implementation(commonLibs.hypertrace.framework.grpc.jakarta)
  implementation(commonLibs.hypertrace.framework.http.jakarta)
  implementation(commonLibs.hypertrace.framework.documentstore.metrics)

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
  integrationTestImplementation(projects.rateLimitingConfigServiceApi)
  integrationTestImplementation(projects.anomalyScoringConfigServiceApi)
  integrationTestImplementation(projects.regionConfigServiceApi)
  integrationTestImplementation(projects.riskConfigServiceApi)
  integrationTestImplementation(projects.sensitiveDataConfigServiceApi)
  integrationTestImplementation(projects.dataClassificationConfigServiceApi)
  integrationTestImplementation(projects.maliciousSourcesConfigServiceApi)
  integrationTestImplementation(projects.detectionExclusionConfigServiceApi)
  integrationTestImplementation(projects.splunkIntegrationConfigServiceApi)
  integrationTestImplementation(testFixtures(projects.fraudDatamodelConfigServiceApi))
  integrationTestImplementation(projects.fraudDatamodelConfigServiceImpl)
  integrationTestImplementation(projects.fraudDatamodelDerivationConfigServiceApi)
  integrationTestImplementation(projects.fraudDatamodelDerivationConfigServiceImpl)
  integrationTestImplementation(projects.fraudPolicyConfigServiceApi)
  integrationTestImplementation(projects.modsecurityUtils)
  integrationTestImplementation(projects.fraudPolicyConfigServiceImpl)
  integrationTestImplementation(commonLibs.traceable.opadistributor.api)
  integrationTestImplementation(localLibs.hypertrace.configservice.partitioner.config.impl)
  integrationTestImplementation(commonLibs.commons.lang)
  integrationTestImplementation(commonLibs.traceable.modsecurity.jni)
  integrationTestImplementation(commonLibs.traceable.coraza.wafServiceClient)
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
