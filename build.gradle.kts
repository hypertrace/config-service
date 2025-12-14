plugins {
  alias(commonLibs.plugins.hypertrace.ciutils)
  alias(commonLibs.plugins.hypertrace.codestyle) apply false
  alias(commonLibs.plugins.hypertrace.publish) apply false
  alias(commonLibs.plugins.owasp.dependencycheck)
}

subprojects {
  group = "org.hypertrace.config.service"
  pluginManager.withPlugin(rootProject.commonLibs.plugins.hypertrace.publish.get().pluginId) {
    configure<org.hypertrace.gradle.publishing.HypertracePublishExtension> {
      license.set(org.hypertrace.gradle.publishing.License.TRACEABLE_COMMUNITY)
    }
  }

  pluginManager.withPlugin("java") {
    configure<JavaPluginExtension> {
      sourceCompatibility = JavaVersion.VERSION_11
      targetCompatibility = JavaVersion.VERSION_11
    }
  }
  apply(plugin = rootProject.commonLibs.plugins.hypertrace.codestyle.get().pluginId)

  configurations.all {
    resolutionStrategy.dependencySubstitution {
      substitute(module("at.yawk.lz4:lz4-java")).using(module("org.lz4:lz4-java:1.8.1"))
        .because("Resolve capability conflict between org.lz4:lz4-java and at.yawk.lz4:lz4-java")
    }
  }
}

dependencyCheck {
  format = org.owasp.dependencycheck.reporting.ReportGenerator.Format.ALL.toString()
  suppressionFile = "owasp-suppressions.xml"
  scanConfigurations.add("runtimeClasspath")
  failBuildOnCVSS = 7.0F
  analyzers.ossIndex.warnOnlyOnRemoteErrors = true
}
