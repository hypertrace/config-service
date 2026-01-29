import com.google.protobuf.gradle.id

plugins {
  `java-library`
  `java-test-fixtures`
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
  api(projects.fraudDatamodelConfigServiceApi)
  api(projects.traceableEdgeDecisionConfigServiceApi)
  api(projects.graphqlAutogenSchemaAnnotations)
  api(commonLibs.bundles.grpc.api)

  testImplementation(commonLibs.protobuf.javautil)
  testImplementation(commonLibs.junit.jupiter)
  testImplementation(commonLibs.mockito.core)
  testImplementation(commonLibs.mockito.junit)
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
