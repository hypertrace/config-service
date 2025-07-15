package ai.traceable.cloud.edge.deployment.config.service.v1.shared.config;

import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigAccessType;
import ai.traceable.cloud.edge.deployment.config.service.v1.ConfigValueDescriptor;
import ai.traceable.cloud.edge.deployment.config.service.v1.SharedConfigMetadata;
import com.google.inject.Inject;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SharedConfigMetadataRegistryImpl implements SharedConfigMetadataRegistry {
  static final String CLUSTER_SHARED_CONFIG_METADATA_FILE_PATH =
      "cluster_shared_config_metadata.yaml";
  static final String SERVICE_SHARED_CONFIG_METADATA_FILE_PATH =
      "service_shared_config_metadata.yaml";
  private final Map<String, ConfigValueDescriptor> clusterConfigValueDescriptorMap;
  private final Map<String, ConfigValueDescriptor> serviceConfigValueDescriptorMap;

  @Inject
  public SharedConfigMetadataRegistryImpl(SharedConfigMetadataParser sharedConfigMetadataParser) {
    clusterConfigValueDescriptorMap =
        sharedConfigMetadataParser
            .loadConfigValueDescriptors(CLUSTER_SHARED_CONFIG_METADATA_FILE_PATH)
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    ConfigValueDescriptor::getConfigKey, Function.identity()));
    serviceConfigValueDescriptorMap =
        sharedConfigMetadataParser
            .loadConfigValueDescriptors(SERVICE_SHARED_CONFIG_METADATA_FILE_PATH)
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    ConfigValueDescriptor::getConfigKey, Function.identity()));
  }

  @Override
  public SharedConfigMetadata getSharedConfigMetadataWithWritePermission(
      ConfigAccessType accessType) {
    return SharedConfigMetadata.newBuilder()
        .putAllClusterConfigDetails(
            clusterConfigValueDescriptorMap.entrySet().stream()
                .filter(
                    entry ->
                        accessType.equals(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                            || entry.getValue().getConfigPermission().getWrite().equals(accessType))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)))
        .putAllServiceConfigDetails(
            serviceConfigValueDescriptorMap.entrySet().stream()
                .filter(
                    entry ->
                        accessType.equals(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                            || entry.getValue().getConfigPermission().getWrite().equals(accessType))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)))
        .build();
  }

  @Override
  public SharedConfigMetadata getSharedConfigMetadataWithReadPermission(
      ConfigAccessType accessType) {

    return SharedConfigMetadata.newBuilder()
        .putAllClusterConfigDetails(
            clusterConfigValueDescriptorMap.entrySet().stream()
                .filter(
                    entry ->
                        accessType.equals(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                            || entry.getValue().getConfigPermission().getRead().equals(accessType))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)))
        .putAllServiceConfigDetails(
            serviceConfigValueDescriptorMap.entrySet().stream()
                .filter(
                    entry ->
                        accessType.equals(ConfigAccessType.CONFIG_ACCESS_TYPE_TRACEABLE)
                            || entry.getValue().getConfigPermission().getRead().equals(accessType))
                .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, Map.Entry::getValue)))
        .build();
  }
}
