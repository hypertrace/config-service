package ai.traceable.saved.query.config.service;

import com.typesafe.config.Config;
import javax.inject.Inject;
import lombok.Getter;

@Getter
public class SavedQueryDataMigrationConfig {

  private static final String IAM_V2_SERVICE_HOST = "iam.v2.service.host";
  private static final String IAM_V2_SERVICE_GRPC_PORT = "iam.v2.service.grpc.port";
  private static final String IAM_V2_SERVICE_TIMEOUT = "iam.v2.service.timeout";
  private static final String SAVED_QUERY_USER_DATA_MIGRATION_FLAG =
      "saved.query.config.service.userDataMigrationFlag";

  private final String iamV2ServiceHost;
  private final int iamV2ServiceGrpcPort;
  private final boolean isSavedQueryUserDataMigrationEnabled;
  private final long iamV2ServiceRequestTimeoutDuration;

  @Inject
  public SavedQueryDataMigrationConfig(Config config) {
    this.iamV2ServiceHost = config.getString(IAM_V2_SERVICE_HOST);
    this.iamV2ServiceGrpcPort = config.getInt(IAM_V2_SERVICE_GRPC_PORT);
    this.isSavedQueryUserDataMigrationEnabled =
        config.getBoolean(SAVED_QUERY_USER_DATA_MIGRATION_FLAG);
    this.iamV2ServiceRequestTimeoutDuration = config.getDuration(IAM_V2_SERVICE_TIMEOUT).toMillis();
  }
}
