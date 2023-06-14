plugins {
  `java-library`
}

dependencies {
  api(libs.hypertrace.grpcutils.context)
  api(libs.grpc.api)
  implementation(libs.protobuf.javautil)
  implementation(libs.seancfoley.ipaddress)

  compileOnly(libs.lombok)
  annotationProcessor(libs.lombok)

  testImplementation(libs.junit.jupiter)

  testCompileOnly(libs.lombok)
  testAnnotationProcessor(libs.lombok)
}

tasks.test {
  useJUnitPlatform()
}
