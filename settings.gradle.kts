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
include(":rate-limiting-config-service-api")
include(":rate-limiting-config-service-impl")
include(":local-processing-config-service-api")
include(":local-processing-config-service-impl")
include(":blocking-config-service-api")
include(":blocking-config-service-impl")
include(":region-config-service-api")
include(":region-config-service-impl")
include(":custom-signature-config-service-api")
include(":custom-signature-config-service-impl")
include(":user-attribution-config-service-api")
include(":user-attribution-config-service-impl")
include(":threat-management-config-service-api")
include(":threat-management-config-service-impl")
include(":external-user-attribution-config-service-api")
include(":external-user-attribution-config-service-impl")
include(":anomaly-config-service-api")
include(":anomaly-config-service-impl")
include(":iprange-config-service-api")
include(":iprange-config-service-impl")
include(":activity-event-producer")
