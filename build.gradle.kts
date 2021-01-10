plugins {
  id("ai.traceable.repository-plugin") version "1.2.2"
  id("org.hypertrace.ci-utils-plugin") version "0.2.0"
  id("ai.traceable.publish-plugin") version "1.2.2" apply false
  id("org.hypertrace.jacoco-report-plugin") version "0.1.3" apply false
  id("org.sonarqube") version "3.0"
  id("org.owasp.dependencycheck") version "6.0.3"
}

subprojects {
  group = "ai.traceable.config.service"
}

dependencyCheck {
  format = org.owasp.dependencycheck.reporting.ReportGenerator.Format.valueOf("ALL")
}
