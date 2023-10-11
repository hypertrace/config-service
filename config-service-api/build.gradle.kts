import com.google.protobuf.gradle.generateProtoTasks
import com.google.protobuf.gradle.id
import com.google.protobuf.gradle.ofSourceSet
import com.google.protobuf.gradle.plugins
import com.google.protobuf.gradle.protobuf
import com.google.protobuf.gradle.protoc

plugins {
  `java-library`
  `java-test-fixtures`
  id("com.google.protobuf")
  id("org.hypertrace.publish-plugin")
}

val generateLocalGoGrpcFiles = false

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:${libs.versions.protoc.get()}"
  }
  plugins {
    // Optional: an artifact spec for a protoc plugin, with "grpc" as
    // the identifier, which can be referred to in the "plugins"
    // container of the "generateProtoTasks" closure.
    id("grpc_java") {
      artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}"
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
  api(libs.bundles.grpc.api)

  testFixturesApi(libs.grpc.api)
  testFixturesApi(projects.configServiceApi)
  testFixturesImplementation(libs.grpc.stub)
  testFixturesImplementation(libs.grpc.core)
  testFixturesImplementation(libs.hypertrace.grpcutils.context)
  testFixturesImplementation(libs.mockito.core)
  testFixturesImplementation(libs.guava)
  testFixturesAnnotationProcessor(libs.lombok)
  testFixturesCompileOnly(libs.lombok)
  constraints {
    implementation("com.google.guava:guava:32.1.2-jre") {
      because("https://nvd.nist.gov/vuln/detail/CVE-2023-2976")
    }
  }
}
