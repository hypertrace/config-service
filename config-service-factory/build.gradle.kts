plugins {
  `java-library`
}

dependencies {
  api(commonLibs.hypertrace.framework.grpc)

  implementation(projects.configServiceChangeEventGenerator)
  implementation(commonLibs.hypertrace.documentstore)
  implementation(projects.configServiceImpl)
  implementation(projects.spacesConfigServiceImpl)
  implementation(projects.labelsConfigServiceImpl)
  implementation(projects.labelApplicationRuleConfigServiceImpl)
  implementation(projects.alertingConfigServiceImpl)
  implementation(projects.notificationRuleConfigServiceImpl)
  implementation(projects.notificationChannelConfigServiceImpl)
  implementation(projects.spanProcessingConfigServiceImpl)
}
