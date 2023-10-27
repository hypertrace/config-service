import org.hypertrace.gradle.dependency.DependencyPluginSettingExtension

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
  id("org.hypertrace.dependency-settings") version "0.1.1"
}

configure<DependencyPluginSettingExtension> {
  catalogVersion.set("0.2.9")
}

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

include(":config-service-change-event-api")
include(":config-service-change-event-generator")

include(":config-service-api")
include(":config-service-impl")
include(":config-service")
include(":config-service-factory")

include(":config-proto-converter")
include(":object-store")
include(":validation-utils")

include(":spaces-config-service-api")
include(":spaces-config-service-impl")

include(":labels-config-service-api")
include(":labels-config-service-impl")

include(":alerting-config-service-api")
include(":alerting-config-service-impl")

include(":label-application-rule-config-service-api")
include(":label-application-rule-config-service-impl")

include(":notification-rule-config-service-api")
include(":notification-rule-config-service-impl")

include(":notification-channel-config-service-api")
include(":notification-channel-config-service-impl")

include(":span-processing-config-service-api")
include(":span-processing-config-service-impl")

include(":span-processing-utils")

include(":partitioner-config-service-api")
include(":partitioner-config-service-impl")
