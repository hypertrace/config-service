plugins {
  `java-library`
  alias(commonLibs.plugins.google.protobuf)
  alias(commonLibs.plugins.hypertrace.publish)
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
  api(commonLibs.protobuf.java)
  api(platform(commonLibs.hypertrace.kafka.bom))
  api(commonLibs.kafka.clients)
}
