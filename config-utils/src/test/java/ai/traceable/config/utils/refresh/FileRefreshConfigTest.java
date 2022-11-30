package ai.traceable.config.utils.refresh;

import static org.junit.jupiter.api.Assertions.*;

import ai.traceable.config.utils.refresh.FileRefreshConfig.Mode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class FileRefreshConfigTest {
  private static final String MOCK_CONFIG =
      "resource {\n"
          + "    mode = RESOURCE_FILE\n"
          + "    resource.file = folder/file.csv\n"
          + "  }\n"
          + "versions {\n"
          + "    mode = VERSIONS_DIR\n"
          + "    versions.dir = /var/version/dir\n"
          + "    versions.file.name = file.csv\n"
          + "    versions.refresh.duration = 24h\n"
          + "}";

  @Test
  void shouldParseConfig() {
    Config mockConfig = ConfigFactory.parseString(MOCK_CONFIG);
    FileRefreshConfig config = new FileRefreshConfig(mockConfig.getConfig("resource"));
    assertEquals(Mode.RESOURCE_FILE, config.getMode());
    assertEquals("folder/file.csv", config.getResourceFile());
    assertNull(config.getVersionsDir());
    assertNull(config.getVersionsFileName());
    assertNull(config.getVersionRefreshDuration());
    config = new FileRefreshConfig(mockConfig.getConfig("versions"));
    assertEquals(Mode.VERSIONS_DIR, config.getMode());
    assertNull(config.getResourceFile());
    assertEquals("/var/version/dir", config.getVersionsDir());
    assertEquals("file.csv", config.getVersionsFileName());
    assertEquals(Duration.ofHours(24), config.getVersionRefreshDuration());
  }
}
