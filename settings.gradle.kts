rootProject.name = "config-service-root"

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

enableFeaturePreview("VERSION_CATALOGS")
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

include(":config-service-api")
include(":config-service-impl")
include(":config-service")

include(":config-proto-converter")

include(":spaces-config-service-api")
include(":spaces-config-service-impl")

include(":labels-config-service-api")
include(":labels-config-service-impl")
include(":label-application-rule-config-service-api")
