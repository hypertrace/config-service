plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.hypertrace.jacoco)
  alias(commonLibs.plugins.google.protobuf)
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:${commonLibs.versions.protoc.get()}"
  }
}

dependencies {
  api(commonLibs.guice7)
  api(commonLibs.typesafe.config)
  api(commonLibs.jakarta.servlet)
  api(commonLibs.bundles.grpc.api)
  api(platform(commonLibs.grpc.bom))

  implementation(commonLibs.bytebuddy)
  implementation(commonLibs.jakarta.jaxrs)
  implementation(commonLibs.guice7.servlet)
  implementation(commonLibs.guava)
  implementation(commonLibs.hypertrace.grpcutils.client)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.grpc.api)
  implementation(commonLibs.protobuf.java)
  implementation(commonLibs.protobuf.javautil)
  implementation(commonLibs.slf4j2.api)
  implementation(platform(commonLibs.jersey.bom.jakarta))
  implementation(commonLibs.jersey.servlet)
  implementation(commonLibs.jersey.inject)
  implementation(commonLibs.jetty.http)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
  // additional api modules that hold the rpc contracts
  testImplementation(commonLibs.traceable.edge.decision.engine.api)
  testImplementation(commonLibs.traceable.fraud.engine.api)
  testImplementation(projects.anomalyConfigServiceApi)
  testImplementation(projects.apiAttributeOverrideServiceApi)

  // Required for the GRPC clients.
  testRuntimeOnly(commonLibs.grpc.netty)
  testRuntimeOnly(commonLibs.grpc.core)
}

tasks.test {
  useJUnitPlatform()
}
