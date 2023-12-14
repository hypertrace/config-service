plugins {
  `java-library`
}

dependencies {
  api(commonLibs.hypertrace.grpcutils.context)
  api(commonLibs.grpc.api)
  implementation(commonLibs.protobuf.javautil)
  implementation(localLibs.seancfoley.ipaddress)
  implementation(commonLibs.google.re2j)
}
