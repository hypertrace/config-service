package ai.traceable.config.utils.refresh;

import com.typesafe.config.Config;
import java.time.Duration;
import lombok.Value;

@Value
public class FileRefreshConfig {
  public enum Mode {
    RESOURCE_FILE,
    VERSIONS_DIR
  }

  private static final String MODE_CONFIG_KEY = "mode";
  private static final String VERSIONS_DIR_CONFIG_KEY = "versions.dir";
  private static final String VERSIONS_FILE_NAME_CONFIG_KEY = "versions.file.name";
  private static final String VERSIONS_REFRESH_DURATION_CONFIG_KEY = "versions.refresh.duration";
  private static final String RESOURCE_FILE_CONFIG_KEY = "resource.file";
  Mode mode;
  String versionsDir;
  String versionsFileName;
  Duration versionRefreshDuration;
  String resourceFile; // relative path of file under resources

  public FileRefreshConfig(Config dataConfig) {
    switch (dataConfig.getEnum(Mode.class, MODE_CONFIG_KEY)) {
      case RESOURCE_FILE:
        this.mode = Mode.RESOURCE_FILE;
        this.resourceFile = dataConfig.getString(RESOURCE_FILE_CONFIG_KEY);
        this.versionsDir = null;
        this.versionsFileName = null;
        this.versionRefreshDuration = null;
        break;
      case VERSIONS_DIR:
        this.mode = Mode.VERSIONS_DIR;
        this.versionsDir = dataConfig.getString(VERSIONS_DIR_CONFIG_KEY);
        this.versionsFileName = dataConfig.getString(VERSIONS_FILE_NAME_CONFIG_KEY);
        this.versionRefreshDuration = dataConfig.getDuration(VERSIONS_REFRESH_DURATION_CONFIG_KEY);
        this.resourceFile = null;
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported data config mode for VersionBasedRefreshConfig");
    }
  }
}
