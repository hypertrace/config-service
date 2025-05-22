package ai.traceable.anomaly.config.service.global.version;

import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersions;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersionsFilter;
import ai.traceable.anomaly.config.service.v1.global.RuleType;
import ai.traceable.anomaly.config.service.v1.global.RuleVersion;
import ai.traceable.anomaly.config.service.v1.global.RuleVersionType;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRuleAvailableVersions;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRuleAvailableVersionsFilter;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRulesVersionType;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectionRulesProvider;
import ai.traceable.protection.rules.webapp.v1.WebAppProtectionRulesProvider;
import ai.traceable.protection.rules.webapp.v1.WebAppRuleAvailableVersions;
import ai.traceable.protection.rules.webapp.v1.WebAppRuleAvailableVersionsFilter;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesVersionType;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.List;
import java.util.stream.Collectors;

public class RuleVersionManagerImpl implements RuleVersionManager {

  private final WebAppProtectionRulesProvider webAppProtectionRulesProvider;
  private final ApiProtectionRulesProvider apiProtectionRulesProvider;
  private static final BiMap<RuleVersionType, WebAppRulesVersionType> WEB_APP_VERSION_MAP =
      ImmutableBiMap.<RuleVersionType, WebAppRulesVersionType>builder()
          .put(
              RuleVersionType.RULE_VERSION_TYPE_STABLE,
              WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_STABLE)
          .put(
              RuleVersionType.RULE_VERSION_TYPE_BETA,
              WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_BETA)
          .put(
              RuleVersionType.RULE_VERSION_TYPE_EXPERIMENTAL,
              WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_EXPERIMENTAL)
          .put(
              RuleVersionType.RULE_VERSION_TYPE_DEPRECATED,
              WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_DEPRECATED)
          .put(
              RuleVersionType.RULE_VERSION_TYPE_UNSPECIFIED,
              WebAppRulesVersionType.WEB_APP_RULES_VERSION_TYPE_UNSPECIFIED)
          .build();

  private static final BiMap<RuleVersionType, ApiProtectRulesVersionType> API_PROTECT_VERSION_MAP =
      ImmutableBiMap.<RuleVersionType, ApiProtectRulesVersionType>builder()
          .put(
              RuleVersionType.RULE_VERSION_TYPE_STABLE,
              ApiProtectRulesVersionType.API_PROTECT_RULES_VERSION_TYPE_STABLE)
          .put(
              RuleVersionType.RULE_VERSION_TYPE_BETA,
              ApiProtectRulesVersionType.API_PROTECT_RULES_VERSION_TYPE_BETA)
          .put(
              RuleVersionType.RULE_VERSION_TYPE_DEPRECATED,
              ApiProtectRulesVersionType.API_PROTECT_RULES_VERSION_TYPE_DEPRECATED)
          .put(
              RuleVersionType.RULE_VERSION_TYPE_UNSPECIFIED,
              ApiProtectRulesVersionType.API_PROTECT_RULES_VERSION_TYPE_UNSPECIFIED)
          .build();

  @Inject
  public RuleVersionManagerImpl(
      WebAppProtectionRulesProvider webAppProtectionRulesProvider,
      ApiProtectionRulesProvider apiProtectionRulesProvider) {
    this.webAppProtectionRulesProvider = webAppProtectionRulesProvider;
    this.apiProtectionRulesProvider = apiProtectionRulesProvider;
  }

  @Override
  public AvailableRuleVersions getAvailableRuleVersions(
      RuleType ruleType, AvailableRuleVersionsFilter filter) {
    AvailableRuleVersions.Builder builder =
        AvailableRuleVersions.newBuilder().setRuleType(ruleType);
    switch (ruleType) {
      case RULE_TYPE_WEB_APPLICATION:
        return builder.addAllVersions(getWebAppRuleVersions(filter)).build();
      case RULE_TYPE_API_PROTECTION:
        return builder.addAllVersions(getApiProtectRuleVersions(filter)).build();
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Invalid rule type: " + ruleType)
            .asRuntimeException();
    }
  }

  private List<RuleVersion> getWebAppRuleVersions(AvailableRuleVersionsFilter filter) {
    WebAppRuleAvailableVersionsFilter webAppFilter =
        WebAppRuleAvailableVersionsFilter.newBuilder()
            .addAllVersionTypes(convertToWebAppRulesVersionType(filter.getVersionTypesList()))
            .build();
    return convertToRuleVersionType(
        webAppProtectionRulesProvider.getWebAppRuleAvailableVersions(webAppFilter));
  }

  private List<RuleVersion> getApiProtectRuleVersions(AvailableRuleVersionsFilter filter) {
    ApiProtectRuleAvailableVersionsFilter apiProtectFilter =
        ApiProtectRuleAvailableVersionsFilter.newBuilder()
            .addAllVersionTypes(convertToApiProtectRulesVersionType(filter.getVersionTypesList()))
            .build();
    return convertToRuleVersionType(
        apiProtectionRulesProvider.getApiProtectRuleAvailableVersions(apiProtectFilter));
  }

  private List<WebAppRulesVersionType> convertToWebAppRulesVersionType(
      List<RuleVersionType> types) {
    return types.stream().map(WEB_APP_VERSION_MAP::get).collect(Collectors.toList());
  }

  private List<ApiProtectRulesVersionType> convertToApiProtectRulesVersionType(
      List<RuleVersionType> types) {
    return types.stream().map(API_PROTECT_VERSION_MAP::get).collect(Collectors.toList());
  }

  private List<RuleVersion> convertToRuleVersionType(WebAppRuleAvailableVersions versions) {
    return versions.getOrderedVersionsList().stream()
        .map(
            version ->
                RuleVersion.newBuilder()
                    .setVersion(version.getVersion())
                    .setVersionType(
                        WEB_APP_VERSION_MAP
                            .inverse()
                            .getOrDefault(
                                version.getVersionType(),
                                RuleVersionType.RULE_VERSION_TYPE_UNSPECIFIED))
                    .build())
        .collect(Collectors.toList());
  }

  private List<RuleVersion> convertToRuleVersionType(ApiProtectRuleAvailableVersions versions) {
    return versions.getOrderedVersionsList().stream()
        .map(
            version ->
                RuleVersion.newBuilder()
                    .setVersion(version.getVersion())
                    .setVersionType(
                        API_PROTECT_VERSION_MAP
                            .inverse()
                            .getOrDefault(
                                version.getVersionType(),
                                RuleVersionType.RULE_VERSION_TYPE_UNSPECIFIED))
                    .build())
        .collect(Collectors.toList());
  }
}
