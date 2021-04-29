plugins {
  `java-library`
  id("org.hypertrace.publish-plugin")
}

dependencies {
  api("io.grpc:grpc-protobuf:1.37.0")
  implementation("com.google.protobuf:protobuf-java-util:3.15.7")
}
