plugins {
  `java-library`
  jacoco
  id("org.hypertrace.jacoco-report-plugin")
}

dependencies {
  api(projects.configServiceChangeEventApi)
  api(libs.typesafe.config)
  api(libs.hypertrace.grpcutils.context)

  implementation(projects.configProtoConverter)
  implementation(libs.hypertrace.eventstore)
  implementation(libs.guava)
  implementation(libs.slf4j.api)

  annotationProcessor(libs.lombok)
  compileOnly(libs.lombok)

  constraints {
    implementation(libs.jersey.common) {
      because("https://snyk.io/vuln/SNYK-JAVA-ORGGLASSFISHJERSEYCORE-1255637")
    }
    implementation(libs.commons.compress) {
      because("Multiple vulnerabilities")
    }
    implementation(libs.kotlin.stdlib) {
      because("https://nvd.nist.gov/vuln/detail/CVE-2020-29582")
    }
    implementation(libs.kotlin.stdlibJdk7) {
      because("https://nvd.nist.gov/vuln/detail/CVE-2020-29582")
    }
    implementation(libs.kotlin.stdlibJdk8) {
      because("https://nvd.nist.gov/vuln/detail/CVE-2020-29582")
    }
  }

  testImplementation(libs.junit.jupiter)
  testImplementation(libs.mockito.core)
  testImplementation(libs.mockito.junit)
  testRuntimeOnly(libs.slf4j.log4jimpl)
}

tasks.test {
  useJUnitPlatform()
}
