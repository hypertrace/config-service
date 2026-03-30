plugins {
  `java-library`
  jacoco
  alias(commonLibs.plugins.google.protobuf)
  alias(commonLibs.plugins.hypertrace.jacoco)
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:${commonLibs.versions.protoc.get()}"
  }
}

sourceSets {
  main {
    java {
      srcDirs("build/generated/source/proto/main/java")
    }
  }
}

dependencies {
  implementation(projects.fraudDatamodelConfigServiceApi)
  implementation(localLibs.hypertrace.configservice.changeeventgenerator)
  implementation(commonLibs.protobuf.java)
  implementation(commonLibs.protobuf.javautil)

  implementation(projects.configProtoUtils)
  implementation(commonLibs.jackson.databind)
  implementation(commonLibs.guice7)
  implementation(commonLibs.slf4j2.api)
  implementation(commonLibs.typesafe.config)
  implementation(commonLibs.hypertrace.documentstore)
  implementation(commonLibs.hypertrace.grpcutils.context)
  implementation(commonLibs.hypertrace.attributeservice.api)

  annotationProcessor(commonLibs.lombok)
  compileOnly(commonLibs.lombok)
  implementation(commonLibs.hypertrace.grpcutils.client)

  testImplementation(commonLibs.bundles.junit.mockito)
  testImplementation(testFixtures(projects.fraudDatamodelConfigServiceApi))
  testImplementation(testFixtures(localLibs.hypertrace.configservice.api))
}

tasks.test {
  useJUnitPlatform()
}
