plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
}

dependencies {
  api(libs.grpc.protobuf)
  implementation(libs.protobuf.javautil)
  constraints {
    implementation(libs.gson) {
      because("https://snyk.io/vuln/SNYK-JAVA-COMGOOGLECODEGSON-1730327")
    }
  }
  constraints {
    implementation("com.google.guava:guava:32.1.2-jre") {
      because("https://nvd.nist.gov/vuln/detail/CVE-2023-2976")
    }
  }
}
