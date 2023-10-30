plugins {
  java
  application
  alias(commonLibs.plugins.hypertrace.docker.application)
  alias(commonLibs.plugins.hypertrace.docker.publish)
  alias(commonLibs.plugins.traceable.docker)
}

dependencies {
  runtimeOnly(commonLibs.grpc.netty)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.slf4j2.api)
  runtimeOnly(commonLibs.log4j.slf4j2.impl)
  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

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
