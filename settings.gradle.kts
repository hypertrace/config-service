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
  id("org.hypertrace.version-settings") version "0.2.0"
}

rootProject.name = "traceable-config-service-root"

enableFeaturePreview("VERSION_CATALOGS")
enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

includeBuild("./hypertrace-config-service")
include(":traceable-config-service")
include(":sensitive-data-config-service-api")
include(":sensitive-data-config-service-impl")
include(":rate-limiting-config-service-api")
include(":rate-limiting-config-service-impl")
include(":license-status-config-service-api")
include(":license-status-config-service-impl")
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
include(":auth-detection-config-service-api")
include(":auth-detection-config-service-impl")
include(":threat-management-config-service-api")
include(":threat-management-config-service-impl")
include(":external-user-attribution-config-service-api")
include(":external-user-attribution-config-service-impl")
include(":external-agent-attribute-config-service-api")
include(":external-agent-attribute-config-service-impl")
include(":anomaly-config-service-api")
include(":anomaly-config-service-impl")
include(":anomaly-config-service-registry")
include(":anomaly-config-service-utils")
include(":iprange-config-service-api")
include(":iprange-config-service-impl")
include(":malicious-sources-config-service-api")
include(":malicious-sources-config-service-impl")
include(":activity-event-producer")
include(":risk-config-service-api")
include(":risk-config-service-impl")
include(":config-utils")
include(":data-config-service-api")
include(":data-config-service-impl")
include(":traceable-alerting-config-service-api")
include(":traceable-alerting-config-service-impl")
include(":data-classification-config-service-api")
include(":data-classification-config-service-impl")
include(":reporting-config-service-api")
include(":reporting-config-service-impl")
include(":data-exfiltration-config-service-api")
include(":data-exfiltration-config-service-impl")
include(":external-data-classification-config-service-api")
include(":external-data-classification-config-service-impl")
include(":api-attribute-override-service-api")
include(":api-attribute-override-service-impl")
include(":waf-provider-integration-service-api")
include(":waf-provider-integration-service-impl")
include(":traceable-span-processing-config-service-api")
include(":traceable-span-processing-config-service-impl")
include(":api-spec-config-service-api")
include(":api-spec-config-service-impl")
include(":feature-caching-client")
include(":traceable-config-service-factory")

include(":mock-config-service")
include(":ast-scan-profile-config-service-api")
include(":ast-scan-profile-config-service-impl")