plugins {
  java
  application
  id("org.hypertrace.docker-java-application-plugin")
  id("org.hypertrace.docker-publish-plugin")
  id("ai.traceable.docker-convention-plugin")
}

dependencies {
  runtimeOnly(libs.grpc.netty)
  implementation(libs.protobuf.javautil)
  implementation(libs.typesafe.config)
  implementation(libs.slf4j.api)
  runtimeOnly(libs.slf4j.log4jimpl)
  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  implementation(projects.sensitiveDataConfigServiceApi)
  implementation(projects.localProcessingConfigServiceApi)
  implementation(projects.blockingConfigServiceApi)
  implementation(projects.externalUserAttributionConfigServiceApi)
  implementation(projects.externalDataClassificationConfigServiceApi)
  implementation(projects.externalAgentAttributeConfigServiceApi)
}

application {
  mainClass.set("ai.traceable.mock.config.service.MockConfigServer")
}

hypertraceDocker {
  traceableConvention {
    registry.set(ai.traceable.gradle.DockerConventionRegistry.GHCR)
  }
  defaultImage {
    javaApplication {
      ports.addAll(50102)
    }
  }
}
