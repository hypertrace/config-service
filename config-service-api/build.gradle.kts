import com.google.protobuf.gradle.id

plugins {
  `java-library`
  `java-test-fixtures`
  alias(commonLibs.plugins.google.protobuf)
  alias(commonLibs.plugins.hypertrace.publish)
}

val generateLocalGoGrpcFiles = false

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:${commonLibs.versions.protoc.get()}"
  }
  plugins {
    // Optional: an artifact spec for a protoc plugin, with "grpc" as
    // the identifier, which can be referred to in the "plugins"
    // container of the "generateProtoTasks" closure.
    id("grpc_java") {
      artifact = "io.grpc:protoc-gen-grpc-java:${commonLibs.versions.grpc.get()}"
    }

    if (generateLocalGoGrpcFiles) {
      id("grpc_go") {
        path = "<go-path>/bin/protoc-gen-go"
      }
    }
  }
  generateProtoTasks {
    ofSourceSet("main").forEach {
      it.plugins {
        // Apply the "grpc" plugin whose spec is defined above, without options.
        id("grpc_java")

        if (generateLocalGoGrpcFiles) {
          id("grpc_go")
        }
      }
      it.builtins {
        java

        if (generateLocalGoGrpcFiles) {
          id("go")
        }
      }
    }
  }
}

sourceSets {
  main {
    java {
      srcDirs("build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc_java")
    }
  }
}

dependencies {
  api(commonLibs.bundles.grpc.api)

  testFixturesApi(commonLibs.grpc.api)
  testFixturesApi(projects.configServiceApi)
  testFixturesImplementation(commonLibs.grpc.inprocess)
  testFixturesImplementation(commonLibs.grpc.stub)
  testFixturesImplementation(commonLibs.grpc.core)
  testFixturesImplementation(commonLibs.hypertrace.grpcutils.context)
  testFixturesImplementation(commonLibs.mockito.core)
  testFixturesImplementation(commonLibs.guava)
  testFixturesAnnotationProcessor(commonLibs.lombok)
  testFixturesCompileOnly(commonLibs.lombok)
}
