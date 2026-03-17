package ai.traceable.risk.config.service.v2.contributors;

import ai.traceable.risk.config.service.v2.EntityType;
import ai.traceable.risk.config.service.v2.RiskConfigBuilder;
import ai.traceable.risk.config.service.v2.RiskConfigServiceConfig;
import ai.traceable.risk.config.service.v2.RiskContributorConfigs;
import ai.traceable.risk.config.service.v2.contributors.validator.RiskContributorConfigsValidator;
import io.grpc.Status;
import jakarta.inject.Inject;
import jakarta.inject.Provider;
import jakarta.inject.Singleton;
import java.util.Map;
import lombok.AllArgsConstructor;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultRiskContributorConfigsProvider
    implements Provider<Map<EntityType, RiskContributorConfigs>> {

  private static final String API_CONFIG_FILE = "risk-contributor-configs.conf";
  private static final String MCP_TOOL_CONFIG_FILE = "risk-contributor-configs-mcp-tool.conf";

  private final RiskConfigBuilder<RiskContributorConfigs> riskConfigBuilder;
  private final RiskContributorConfigsValidator contributorConfigsValidator;
  private final RiskConfigServiceConfig riskConfig;

  @Override
  public Map<EntityType, RiskContributorConfigs> get() {
    RiskContributorConfigs apiConfigs =
        riskConfigBuilder.mergeConfigs(riskConfig.getRiskContributorConfigs(), API_CONFIG_FILE);
    RiskContributorConfigs mcpToolConfigs =
        riskConfigBuilder.buildFromConfigFile(MCP_TOOL_CONFIG_FILE);
    validate(apiConfigs);
    validate(mcpToolConfigs);
    return Map.of(
        EntityType.ENTITY_TYPE_API, apiConfigs,
        EntityType.ENTITY_TYPE_MCP_TOOL, mcpToolConfigs);
  }

  private void validate(RiskContributorConfigs configs) {
    Status validationStatus = contributorConfigsValidator.validateRiskContributorConfigs(configs);
    if (!validationStatus.isOk()) {
      throw validationStatus.asRuntimeException();
    }
  }
}
