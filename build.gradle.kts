plugins {
  id("ai.traceable.repository-plugin") version "1.2.2"
  id("org.hypertrace.ci-utils-plugin") version "0.2.0"
  id("ai.traceable.publish-plugin") version "1.2.2" apply false
  id("org.hypertrace.jacoco-report-plugin") version "0.1.3" apply false
  id("org.sonarqube") version "3.0"
  id("org.owasp.dependencycheck") version "6.0.3"
  id("com.diffplug.spotless") version "5.9.0"
}

subprojects {
  group = "ai.traceable.config.service"

  apply(plugin = "com.diffplug.spotless")
  spotless {
    java {
      importOrder()
      removeUnusedImports()
      googleJavaFormat()
      target("src/**/*.java")
    }
    kotlin {
      ktlint().userData(mapOf("indent_size" to "2", "continuation_indent_size" to "2"))
    }

    format("misc,") {
      target(".gitignore", "*.md", "**/*.proto")
      indentWithSpaces()
      trimTrailingWhitespace()
      endWithNewline()
    }
  }
  tasks {
    check {
      dependsOn("spotlessCheck")
    }
  }
}

dependencyCheck {
  format = org.owasp.dependencycheck.reporting.ReportGenerator.Format.valueOf("ALL")
  suppressionFile = "owasp-suppressions.xml"
  scanConfigurations.add("runtimeClasspath")
}
