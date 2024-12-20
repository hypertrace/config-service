package ai.traceable.data.protection.config.service;

import static ai.traceable.data.protection.config.service.DataProtectionConfigStore.DEFAULT_DATA_PROTECTION_CONTEXT;

import ai.traceable.data.protection.config.service.v1.DeleteScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.GetResolvedScopedDataProtectionConfigRequest;
import ai.traceable.data.protection.config.service.v1.ScopedDataProtectionConfig;
import ai.traceable.data.protection.config.service.v1.UpsertScopedDataProtectionConfigRequest;
import jakarta.inject.Inject;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class DataProtectionConfigManager implements ConfigManager {
  private DataProtectionConfigStore dataProtectionConfigStore;

  public Optional<ScopedDataProtectionConfig> getResolvedScopedDataProtectionConfig(
      GetResolvedScopedDataProtectionConfigRequest request, RequestContext requestContext) {

    if (!request.hasScope()) {
      return dataProtectionConfigStore.getData(requestContext, DEFAULT_DATA_PROTECTION_CONTEXT);
    }
    return mergeConfigs(
        dataProtectionConfigStore.getAllConfigData(requestContext, request.getScope()));
  }

  public void deleteScopedDataProtectionConfig(
      DeleteScopedDataProtectionConfigRequest request, RequestContext requestContext) {
    dataProtectionConfigStore.deleteObject(
        requestContext, dataProtectionConfigStore.getContextFromData(request.getScope()));
  }

  public ScopedDataProtectionConfig upsertScopedDataProtectionConfig(
      UpsertScopedDataProtectionConfigRequest request, RequestContext requestContext) {
    ScopedDataProtectionConfig.Builder builder =
        ScopedDataProtectionConfig.newBuilder().setConfig(request.getConfig());
    if (request.hasScope()) {
      builder.setScope(request.getScope());
    }
    return dataProtectionConfigStore.upsertObject(requestContext, builder.build()).getData();
  }

  private Optional<ScopedDataProtectionConfig> mergeConfigs(
      List<ScopedDataProtectionConfig> scopedDataProtectionConfigList) {
    return scopedDataProtectionConfigList.stream()
        .reduce((first, second) -> first.toBuilder().mergeFrom(second).build());
  }
}
