
import com.google.protobuf.gradle.id

plugins {
  `java-library`
  alias(commonLibs.plugins.google.protobuf)
  alias(commonLibs.plugins.traceable.publish)
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
  api(projects.traceableEdgeDecisionConfigServiceApi)
  api(projects.traceableDatamodelConfigServiceApi)

  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.junit)
  testImplementation(commonLibs.protobuf.javautil)
  testImplementation(commonLibs.jackson.yaml)
}

tasks.test {
  useJUnitPlatform()
}

sourceSets {
  main {
    java {
      srcDirs("build/generated/source/proto/main/java", "build/generated/source/proto/main/grpc")
    }
  }
}
