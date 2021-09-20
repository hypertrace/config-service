plugins {
  `java-library`
}

dependencies {
  api(libs.hypertrace.grpcutils.context)
  api(libs.grpc.api)
  implementation(libs.protobuf.javautil)
}