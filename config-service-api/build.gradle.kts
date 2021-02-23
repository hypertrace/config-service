import com.google.protobuf.gradle.*

plugins {
  `java-library`
  `java-test-fixtures`
  id("com.google.protobuf") version "0.8.13"
  id("org.hypertrace.publish-plugin")
}

val generateLocalGoGrpcFiles = false

protobuf {
  protoc {
    artifact = "com.google.protobuf:protoc:3.13.0"
  }
  plugins {
    // Optional: an artifact spec for a protoc plugin, with "grpc" as
    // the identifier, which can be referred to in the "plugins"
    // container of the "generateProtoTasks" closure.
    id("grpc_java") {
      artifact = "io.grpc:protoc-gen-grpc-java:1.35.0"
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
      srcDirs("src/main/java", "build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc_java")
    }
  }
}

dependencies {
  api("io.grpc:grpc-protobuf:1.35.0")
  api("io.grpc:grpc-stub:1.35.0")
  api("javax.annotation:javax.annotation-api:1.3.2")
  constraints {
    implementation("com.google.guava:guava:30.1-jre") {
      because("https://snyk.io/vuln/SNYK-JAVA-COMGOOGLEGUAVA-1015415")
    }
  }
  testFixturesApi("io.grpc:grpc-api:1.35.0")
  testFixturesApi(project(":config-service-api"))
  testFixturesImplementation("io.grpc:grpc-stub:1.35.0")
  testFixturesImplementation("io.grpc:grpc-core:1.35.0")
  testFixturesImplementation("org.hypertrace.core.grpcutils:grpc-context-utils:0.3.3")
  testFixturesImplementation("org.mockito:mockito-core:3.7.0")
  testFixturesImplementation("com.google.guava:guava:30.1-jre")
  testFixturesAnnotationProcessor("org.projectlombok:lombok:1.18.12")
  testFixturesCompileOnly("org.projectlombok:lombok:1.18.12")
}
