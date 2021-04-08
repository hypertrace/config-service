rootProject.name = "config-service"

pluginManagement {
  repositories {
    mavenLocal()
    gradlePluginPortal()
    maven("https://hypertrace.jfrog.io/artifactory/maven")
  }
}

plugins {
  id("org.hypertrace.version-settings") version "0.2.0"
}

include(":config-service-api")
include(":config-service-impl")
include(":config-service")

include(":config-proto-converter")

include(":spaces-config-service-api")
include(":spaces-config-service-impl")
