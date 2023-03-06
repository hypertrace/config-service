package ai.traceable.ast.config.service.rules;

import ai.traceable.ast.config.service.v1.ScanPurgeConfig;
import ai.traceable.ast.config.service.v1.UpdateScanPurgeConfigRequest;
import com.google.inject.Inject;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@AllArgsConstructor(onConstructor_ = {@Inject})
class AstRulesManager implements RulesManager {
  private final ScanPurgeConfigStore scanPurgeConfigStore;

  @Override
  public ScanPurgeConfig updateScanPurgeConfig(
      RequestContext requestContext, UpdateScanPurgeConfigRequest request) {
    ScanPurgeConfig scanPurgeConfig =
        ScanPurgeConfig.newBuilder()
            .setPurgeDuration(request.getPurgeConfig().getPurgeDuration())
            .build();
    return scanPurgeConfigStore.upsertObject(requestContext, scanPurgeConfig).getData();
  }

  @Override
  public Optional<ScanPurgeConfig> getScanPurgeConfig(RequestContext requestContext) {
    return scanPurgeConfigStore.getData(requestContext);
  }
}
