plugins {
  `java-library`
  alias(commonLibs.plugins.traceable.publish)
}

dependencies {
  testImplementation(commonLibs.junit.jupiter)
}
