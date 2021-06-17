package ai.traceable.anomaly.config.service.exclusion;

import static ai.traceable.anomaly.config.service.exclusion.AnomalyExclusionConfigServiceConstants.ANOMALY_EXCLUSION_CONFIG_NAMESPACE;
import static ai.traceable.anomaly.config.service.exclusion.AnomalyExclusionConfigServiceConstants.ANOMALY_EXCLUSION_CONFIG_RESOURCE_NAME;

import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.config.service.v1.ConfigServiceGrpc.ConfigServiceBlockingStub;
import org.hypertrace.config.service.v1.ContextSpecificConfig;
import org.hypertrace.config.service.v1.GetAllConfigsRequest;
import org.hypertrace.config.service.v1.UpsertConfigRequest;

public class ExclusionTestUtils {

  public static Value mockConfig(String id, String name) {
    Struct ruleConfigStruct =
        Struct.newBuilder()
            .putFields("id", Value.newBuilder().setStringValue(id).build())
            .putFields("name", Value.newBuilder().setStringValue(name).build())
            .build();
    return Value.newBuilder().setStructValue(ruleConfigStruct).build();
  }

  public static List<Value> getAllConfigs(ConfigServiceBlockingStub configServiceBlockingStub) {
    return configServiceBlockingStub
        .getAllConfigs(
            GetAllConfigsRequest.newBuilder()
                .setResourceName(ANOMALY_EXCLUSION_CONFIG_RESOURCE_NAME)
                .setResourceNamespace(ANOMALY_EXCLUSION_CONFIG_NAMESPACE)
                .build())
        .getContextSpecificConfigsList()
        .stream()
        .map(ContextSpecificConfig::getConfig)
        .collect(Collectors.toUnmodifiableList());
  }

  public static void upsertConfigs(
      Map<String, Value> exclusionRuleConfigs,
      ConfigServiceBlockingStub configServiceBlockingStub) {
    exclusionRuleConfigs.forEach(
        (id, ruleConfig) ->
            configServiceBlockingStub.upsertConfig(
                UpsertConfigRequest.newBuilder()
                    .setResourceNamespace(ANOMALY_EXCLUSION_CONFIG_NAMESPACE)
                    .setResourceName(ANOMALY_EXCLUSION_CONFIG_RESOURCE_NAME)
                    .setConfig(ruleConfig)
                    .setContext(id)
                    .build()));
  }
}
