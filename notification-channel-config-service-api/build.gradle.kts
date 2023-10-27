import com.google.protobuf.gradle.id

plugins {
  `java-library`
  alias(commonLibs.plugins.google.protobuf)
  alias(commonLibs.plugins.hypertrace.publish)
}

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:${commonLibs.versions.protoc.get()}"
  }
  plugins {
    id("grpc") {
      artifact = "io.grpc:protoc-gen-grpc-java:${commonLibs.versions.grpc.get()}"
    }
  }
  generateProtoTasks {
    ofSourceSet("main").forEach { task ->
      task.plugins {
        id("grpc")
      }
    }
  }
}

dependencies {
  api(commonLibs.bundles.grpc.api)
}

sourceSets {
  main {
    java {
      srcDirs("build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc")
    }
  }
}
