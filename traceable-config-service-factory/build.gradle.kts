plugins {
  `java-library`
}

dependencies {
  api(libs.hypertrace.grpc.framework)

  implementation(libs.hypertrace.configservice.factory)
  implementation(libs.hypertrace.configservice.impl)
  implementation(libs.hypertrace.configservice.changeeventgenerator)
  implementation(libs.typesafe.config)

  implementation(projects.activityEventProducer)
  implementation(projects.sensitiveDataConfigServiceImpl)
  implementation(projects.localProcessingConfigServiceImpl)
  implementation(projects.blockingConfigServiceImpl)
  implementation(projects.externalUserAttributionConfigServiceImpl)
  implementation(projects.externalAgentAttributeConfigServiceImpl)
  implementation(projects.externalDataClassificationConfigServiceImpl)

  implementation(projects.licenseStatusConfigServiceImpl)
  implementation(projects.localProcessingConfigServiceImpl)
  implementation(projects.regionConfigServiceImpl)
  implementation(projects.iprangeConfigServiceImpl)
  implementation(projects.customSignatureConfigServiceImpl)
  implementation(projects.userAttributionConfigServiceImpl)
  implementation(projects.threatManagementConfigServiceImpl)
  implementation(projects.riskConfigServiceImpl)
  implementation(projects.dataClassificationConfigServiceImpl)
  implementation(projects.dataExfiltrationConfigServiceImpl)
  implementation(projects.wafProviderIntegrationServiceImpl)
  implementation(projects.rateLimitingConfigServiceImpl)
  implementation(projects.traceableSpanProcessingConfigServiceImpl)
  implementation(projects.anomalyConfigServiceImpl)
  implementation(projects.apiAttributeOverrideServiceImpl)
  implementation(projects.traceableAlertingConfigServiceImpl)
  implementation(projects.reportingConfigServiceImpl)
  implementation(projects.apiSpecConfigServiceImpl)
  implementation(projects.maliciousSourcesConfigServiceImpl)
  implementation(projects.authDetectionConfigServiceImpl)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)
}
