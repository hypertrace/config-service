plugins {
  id("ai.traceable.repository-plugin") version "1.4.3"
  id("org.hypertrace.ci-utils-plugin") version "0.2.0"
  id("ai.traceable.publish-plugin") version "1.4.3" apply false
  id("org.hypertrace.jacoco-report-plugin") version "0.2.0" apply false
  id("org.sonarqube") version "3.4.0.2513"
  id("org.owasp.dependencycheck") version "8.2.1"
  id("org.hypertrace.code-style-plugin") version "1.1.2" apply false
  id("org.hypertrace.docker-java-application-plugin") version "0.9.9" apply false
  id("org.hypertrace.docker-publish-plugin") version "0.9.9" apply false
  id("ai.traceable.docker-convention-plugin") version "1.4.3" apply false
}

subprojects {
  group = "ai.traceable.config.service"

  apply(plugin = "org.hypertrace.code-style-plugin")
}

dependencyCheck {
  format = org.owasp.dependencycheck.reporting.ReportGenerator.Format.ALL.toString()
  suppressionFile = "owasp-suppressions.xml"
  scanConfigurations.add("runtimeClasspath")
  skipProjects.add(":mock-config-service")
  analyzers.ossIndex.warnOnlyOnRemoteErrors = true
}
