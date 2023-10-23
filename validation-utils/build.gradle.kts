plugins {
  `java-library`
}

dependencies {
  api(commonLibs.hypertrace.grpcutils.context)
  api(commonLibs.grpc.api)
  implementation(commonLibs.protobuf.javautil)
  implementation(libs.seancfoley.ipaddress)
}
