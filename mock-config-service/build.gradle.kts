plugins {
  java
  application
  id("org.hypertrace.docker-java-application-plugin") version "0.9.4"
  id("org.hypertrace.docker-publish-plugin") version "0.9.4"
  id("ai.traceable.docker-convention-plugin") version "1.2.4"
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
