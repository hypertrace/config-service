pluginManagement {
  repositories {
    mavenLocal()
    maven((extra.properties["artifactory_contextUrl"] as String) + "/gradle") {
      credentials {
        username = extra.properties["artifactory_user"] as String
        password = extra.properties["artifactory_password"] as String
      }
    }
  }
}

plugins {
  id("org.hypertrace.version-settings") version "0.1.2"
}

rootProject.name = "traceable-config-service"

includeBuild("./hypertrace-config-service")
include(":traceable-config-service")
include(":sensitive-data-config-service-api")
include(":sensitive-data-config-service-impl")
