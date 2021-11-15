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
}
