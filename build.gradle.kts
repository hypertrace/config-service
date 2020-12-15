plugins {
  id("ai.traceable.repository-plugin") version "1.2.1"
  id("org.hypertrace.ci-utils-plugin") version "0.1.2"
  id("ai.traceable.publish-plugin") version "1.2.1" apply false
  id("org.hypertrace.jacoco-report-plugin") version "0.1.1" apply false
}

subprojects {
  group = "ai.traceable.example"
}
