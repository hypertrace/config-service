plugins {
  `java-library`
  alias(commonLibs.plugins.hypertrace.publish)
}

dependencies {
  api(commonLibs.grpc.protobuf)
  implementation(commonLibs.protobuf.javautil)
}
