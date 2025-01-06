package ai.traceable.fraud.datamodel.derivation.config.service;

import static java.util.function.Function.identity;

import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfig;
import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfigType;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigObject;
import jakarta.inject.Inject;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public class DefaultFraudDataModelDerivationConfig {

  static final String FRAUD_DATAMODEL_DERIVATION_CONFIG_SERVICE =
      "fraud.datamodel.derivation.config.service";
  private static final String DEFAULT_DERIVATION_CONFIGS = "default.derivation.configs";
  private final Map<String, DerivationConfig> defaultDerivationConfigsMap;

  @Inject
  public DefaultFraudDataModelDerivationConfig(Config config) {
    Config derivationConfig = config.getConfig(FRAUD_DATAMODEL_DERIVATION_CONFIG_SERVICE);
    List<? extends ConfigObject> defaultDerivationConfigsObjs =
        derivationConfig.getObjectList(DEFAULT_DERIVATION_CONFIGS);
    this.defaultDerivationConfigsMap =
        defaultDerivationConfigsObjs.stream()
            .map(DefaultFraudDataModelDerivationConfig::buildDerivationConfigFromConfig)
            .collect(ImmutableMap.toImmutableMap(DerivationConfig::getId, identity()));
  }

  public Collection<DerivationConfig> getDerivationConfigsForType(DerivationConfigType type) {
    return defaultDerivationConfigsMap.values().stream()
        .filter(derivationConfig -> type.equals(derivationConfig.getDerivationConfigType()))
        .collect(Collectors.toUnmodifiableList());
  }

  public boolean isDefaultConfig(String id) {
    return defaultDerivationConfigsMap.containsKey(id);
  }

  public Optional<DerivationConfig> getDefaultConfig(String id) {
    return Optional.ofNullable(defaultDerivationConfigsMap.get(id));
  }

  @SneakyThrows
  private static DerivationConfig buildDerivationConfigFromConfig(
      com.typesafe.config.ConfigObject configObject) {
    String jsonString = configObject.render();
    DerivationConfig.Builder builder = DerivationConfig.newBuilder();
    JsonFormat.parser().merge(jsonString, builder);
    return builder.build();
  }
}
