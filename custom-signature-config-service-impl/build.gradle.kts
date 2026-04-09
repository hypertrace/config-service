import com.bmuschko.gradle.docker.tasks.container.DockerCreateContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStartContainer
import com.bmuschko.gradle.docker.tasks.container.DockerStopContainer
import com.bmuschko.gradle.docker.tasks.image.DockerPullImage

plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.hypertrace.docker.application)
  alias(commonLibs.plugins.traceable.docker)
}

dependencies {
  api(commonLibs.grpc.api)
  api(commonLibs.typesafe.config)

  implementation(projects.anomalyConfigServiceRegistry)
  implementation(projects.customSignatureConfigServiceApi)
  implementation(projects.modsecurityUtils)
  implementation(projects.configUtils)
  implementation(projects.auditUtils)
  implementation(projects.featureCachingClient)
  implementation(projects.traceableDatamodelConfigServiceApi)
  implementation(projects.traceableEdgeDecisionConverterUtils)
  implementation(projects.entityFetcherCache)

  implementation(localLibs.hypertrace.configservice.api)
  implementation(commonLibs.guice7)
  implementation(commonLibs.guava)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.uuidcreator)
  implementation(commonLibs.re2j)
  implementation(commonLibs.commons.lang)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(localLibs.hypertrace.configservice.protoconverter)
  implementation(commonLibs.traceable.platform.ipUtils)
  implementation(localLibs.hypertrace.configservice.objectstore)
  implementation(commonLibs.traceable.modsecurity.jni)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(localLibs.hypertrace.configservice.validation)
  implementation(commonLibs.traceable.traceenricher.constants)
  implementation(commonLibs.hypertrace.kafkaStreams.eventListener)
  implementation(commonLibs.jackson.core)
  implementation(commonLibs.jackson.databind)
  implementation(commonLibs.commons.io)
  implementation(commonLibs.traceable.protection.engine.config.customsignature)
  implementation(commonLibs.traceable.protection.engine.processor.secrules)
  implementation(commonLibs.traceable.protection.engine.processor.conditionexpression)
  implementation(commonLibs.traceable.protection.engine.processing.common)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.register<DockerPullImage>("pullCorazaImage") {
  group = "docker"
  description = "Pulls the Coraza WAF service Docker image."
  image.set(docker.registryCredentials.url.get() + "/traceable/coraza-waf-service:${commonLibs.versions.traceable.corazaWafService.get()}")
}

val corazaContainer = "coraza-local"
tasks.register<DockerCreateContainer>("createCorazaContainer") {
  group = "docker"
  description = "Creates the Coraza WAF service container for tests."
  dependsOn("pullCorazaImage")
  containerName.set(corazaContainer)
  targetImageId(tasks.named<DockerPullImage>("pullCorazaImage").get().image)
  hostConfig.apply {
    portBindings.set(listOf("9000:9000"))
    autoRemove.set(true)
  }
}

tasks.register<DockerStartContainer>("startCorazaContainer") {
  group = "docker"
  description = "Starts the Coraza WAF service container before tests."
  dependsOn("createCorazaContainer")
  targetContainerId(tasks.named<DockerCreateContainer>("createCorazaContainer").get().containerId)
}

tasks.register<DockerStopContainer>("stopCorazaContainer") {
  group = "docker"
  description = "Stops the Coraza WAF service container after tests."
  targetContainerId(tasks.named<DockerCreateContainer>("createCorazaContainer").get().containerId)
}

tasks.test {
  useJUnitPlatform()
  dependsOn("startCorazaContainer")
  finalizedBy("stopCorazaContainer")
}
