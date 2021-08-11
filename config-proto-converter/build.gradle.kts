plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
}

dependencies {
  api(libs.grpc.protobuf)
  implementation(libs.protobuf.javautil)
}
