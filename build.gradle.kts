plugins {
  alias(commonLibs.plugins.hypertrace.ciutils)
  alias(commonLibs.plugins.hypertrace.codestyle) apply false
  alias(commonLibs.plugins.owasp.dependencycheck)
  alias(commonLibs.plugins.sonarqube)
  alias(commonLibs.plugins.hypertrace.java.convention)
}

subprojects {
  group = "ai.traceable.config.service"
  pluginManager.withPlugin("java") {
    apply(plugin = commonLibs.plugins.hypertrace.codestyle.get().pluginId)
  }
}

dependencyCheck {
  format = org.owasp.dependencycheck.reporting.ReportGenerator.Format.ALL.toString()
  suppressionFile = "owasp-suppressions.xml"
  scanConfigurations.add("runtimeClasspath")
  skipProjects.add(":mock-config-service")
  failBuildOnCVSS = 7.0F
  analyzers.ossIndex.warnOnlyOnRemoteErrors = true
}
