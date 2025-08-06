package ai.traceable.anomaly.config.service.global.version;

import static ai.traceable.protection.rules.webapp.v1.WebAppThreatRuleType.WEB_APP_THREAT_RULE_TYPE_AGGRESSIVE;

import ai.traceable.anomaly.config.service.v1.ChangeLog;
import ai.traceable.anomaly.config.service.v1.ChangeLogRow;
import ai.traceable.anomaly.config.service.v1.ChangeLogTable;
import ai.traceable.anomaly.config.service.v1.ChangeLogTable.Builder;
import ai.traceable.anomaly.config.service.v1.RuleType;
import ai.traceable.anomaly.config.service.v1.RuleVersion;
import ai.traceable.anomaly.config.service.v1.RuleVersionType;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersions;
import ai.traceable.anomaly.config.service.v1.global.AvailableRuleVersionsFilter;
import ai.traceable.anomaly.config.service.v1.global.RuleVersionUpdateDetails;
import ai.traceable.anomaly.config.service.v1.global.RulesChangeLog;
import ai.traceable.anomaly.config.service.v1.global.StringKeyValueUpdate;
import ai.traceable.anomaly.config.service.v1.global.StringValueUpdate;
import ai.traceable.anomaly.config.service.v1.global.ThreatRuleChange;
import ai.traceable.anomaly.config.service.v1.global.ThreatRuleUpdateDetails;
import ai.traceable.anomaly.config.service.v1.global.ThreatTypeChange;
import ai.traceable.anomaly.config.service.v1.global.ThreatTypeUpdateDetails;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRuleAvailableVersions;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRuleAvailableVersionsFilter;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRulesChangeLog;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRulesVersion;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectRulesVersionType;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatRule;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatRuleChange;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatRuleType;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatRuleUpdateDetails;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatType;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatTypeChange;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatTypeUpdateDetails;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectVersionedRules;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectVersionedRulesFilter;
import ai.traceable.protection.rules.apiprotect.v1.ApiProtectionRulesProvider;
import ai.traceable.protection.rules.webapp.v1.WebAppProtectionRulesProvider;
import ai.traceable.protection.rules.webapp.v1.WebAppRuleAvailableVersions;
import ai.traceable.protection.rules.webapp.v1.WebAppRuleAvailableVersionsFilter;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesChangeLog;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesVersion;
import ai.traceable.protection.rules.webapp.v1.WebAppRulesVersionType;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRule;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRuleChange;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRuleUpdateDetails;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatRuleUpdateDetails.ThreatRuleUpdate;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatType;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatTypeChange;
import ai.traceable.protection.rules.webapp.v1.WebAppThreatTypeUpdateDetails;
import ai.traceable.protection.rules.webapp.v1.WebAppVersionedRules;
import ai.traceable.protection.rules.webapp.v1.WebAppVersionedRulesFilter;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import io.grpc.Status;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

public class RuleVersionManagerImpl implements RuleVersionManager {

  private final WebAppProtectionRulesProvider webAppProtectionRulesProvider;
  private final ApiProtectionRulesProvider apiProtectionRulesProvider;
  private static final String THREAT_TYPE = "Threat Type";
  private static final String THREAT_RULE = "Threat Rule";
  private static final String IS_AGGRESSIVE = "Is Aggressive";
  private static final String IS_STANDARD = "Is Standard";
  private static final String SEVERITY = "Severity";
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

  @Override
  public RulesChangeLog getRulesChangeLog(
      RuleType ruleType, RuleVersion currentVersion, RuleVersion previousVersion) {
    switch (ruleType) {
      case RULE_TYPE_WEB_APPLICATION:
        return getWebAppRulesChangeLog(currentVersion, previousVersion);
      case RULE_TYPE_API_PROTECTION:
        return getApiProtectRulesChangeLog(currentVersion, previousVersion);
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Invalid rule type: " + ruleType)
            .asRuntimeException();
    }
  }

  @Override
  public ChangeLog getChangeLogDoc(
      RuleType ruleType, RuleVersion currentVersion, RuleVersion previousVersion) {
    switch (ruleType) {
      case RULE_TYPE_WEB_APPLICATION:
        return getWebAppChangeLogDoc(currentVersion, previousVersion);
      case RULE_TYPE_API_PROTECTION:
        return getApiProtectChangeLogDoc(currentVersion, previousVersion);
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Invalid rule type: " + ruleType)
            .asRuntimeException();
    }
  }

  private RulesChangeLog getWebAppRulesChangeLog(
      RuleVersion currentVersion, RuleVersion previousVersion) {
    WebAppRulesVersion currentWebAppRulesVersion =
        WebAppRulesVersion.newBuilder()
            .setVersion(currentVersion.getVersion())
            .setVersionType(WEB_APP_VERSION_MAP.get(currentVersion.getVersionType()))
            .build();
    WebAppRulesVersion previousWebAppRulesVersion =
        WebAppRulesVersion.newBuilder()
            .setVersion(previousVersion.getVersion())
            .setVersionType(WEB_APP_VERSION_MAP.get(previousVersion.getVersionType()))
            .build();
    return convertToRuleChangeLog(
        webAppProtectionRulesProvider.getWebAppRulesChangeLog(
            previousWebAppRulesVersion, currentWebAppRulesVersion),
        currentVersion,
        previousVersion);
  }

  private RulesChangeLog getApiProtectRulesChangeLog(
      RuleVersion currentVersion, RuleVersion previousVersion) {
    ApiProtectRulesVersion currentApiProtectRulesVersion =
        ApiProtectRulesVersion.newBuilder()
            .setVersion(currentVersion.getVersion())
            .setVersionType(API_PROTECT_VERSION_MAP.get(currentVersion.getVersionType()))
            .build();
    ApiProtectRulesVersion previousApiProtectRulesVersion =
        ApiProtectRulesVersion.newBuilder()
            .setVersion(previousVersion.getVersion())
            .setVersionType(API_PROTECT_VERSION_MAP.get(previousVersion.getVersionType()))
            .build();
    return convertToRuleChangeLog(
        apiProtectionRulesProvider.getApiProtectRulesChangeLog(
            currentApiProtectRulesVersion, previousApiProtectRulesVersion),
        currentVersion,
        previousVersion);
  }

  private ChangeLog getWebAppChangeLogDoc(RuleVersion currentVersion, RuleVersion previousVersion) {
    WebAppRulesChangeLog webAppRulesChangeLog =
        webAppProtectionRulesProvider.getWebAppRulesChangeLog(
            WebAppRulesVersion.newBuilder()
                .setVersion(previousVersion.getVersion())
                .setVersionType(WEB_APP_VERSION_MAP.get(previousVersion.getVersionType()))
                .build(),
            WebAppRulesVersion.newBuilder()
                .setVersion(currentVersion.getVersion())
                .setVersionType(WEB_APP_VERSION_MAP.get(currentVersion.getVersionType()))
                .build());
    WebAppVersionedRules webAppVersionedRule =
        webAppProtectionRulesProvider
            .getWebAppVersionedRules(
                WebAppVersionedRulesFilter.newBuilder()
                    .addRulesVersions(currentVersion.getVersion())
                    .addRulesVersionTypes(WEB_APP_VERSION_MAP.get(currentVersion.getVersionType()))
                    .build())
            .get(0);

    ChangeLog.Builder builder =
        ChangeLog.newBuilder()
            .setRuleType(RuleType.RULE_TYPE_WEB_APPLICATION)
            .setCurrentVersion(currentVersion)
            .setPreviousVersion(previousVersion)
            .setHighlights(
                getCurrentVersionHighlights(
                    webAppRulesChangeLog
                        .getVersionUpdated()
                        .getNewVersion()
                        .getVersionHighlights()));
    return populateChangeLogTables(webAppRulesChangeLog, webAppVersionedRule, builder);
  }

  private ChangeLog getApiProtectChangeLogDoc(
      RuleVersion currentVersion, RuleVersion previousVersion) {
    ApiProtectRulesChangeLog apiProtectRulesChangeLog =
        apiProtectionRulesProvider.getApiProtectRulesChangeLog(
            ApiProtectRulesVersion.newBuilder()
                .setVersion(currentVersion.getVersion())
                .setVersionType(API_PROTECT_VERSION_MAP.get(currentVersion.getVersionType()))
                .build(),
            ApiProtectRulesVersion.newBuilder()
                .setVersion(previousVersion.getVersion())
                .setVersionType(API_PROTECT_VERSION_MAP.get(previousVersion.getVersionType()))
                .build());
    ApiProtectVersionedRules apiProtectVersionedRule =
        apiProtectionRulesProvider
            .getApiProtectVersionedRules(
                ApiProtectVersionedRulesFilter.newBuilder()
                    .addRuleVersions(currentVersion.getVersion())
                    .build())
            .get(0);

    ChangeLog.Builder builder =
        ChangeLog.newBuilder()
            .setRuleType(RuleType.RULE_TYPE_API_PROTECTION)
            .setCurrentVersion(currentVersion)
            .setPreviousVersion(previousVersion)
            .setHighlights(
                getCurrentVersionHighlights(
                    apiProtectRulesChangeLog
                        .getVersionUpdated()
                        .getNewVersion()
                        .getVersionHighlights()));
    return populateChangeLogTables(apiProtectRulesChangeLog, apiProtectVersionedRule, builder);
  }

  private RulesChangeLog convertToRuleChangeLog(
      WebAppRulesChangeLog webAppRulesChangeLog,
      RuleVersion currentVersion,
      RuleVersion previousVersion) {

    RulesChangeLog.Builder changeLogBuilder =
        RulesChangeLog.newBuilder()
            .setVersionUpdated(getRuleVersionUpdateDetails(currentVersion, previousVersion))
            .setRuleType(RuleType.RULE_TYPE_WEB_APPLICATION);
    if (!webAppRulesChangeLog.getThreatTypeChangesList().isEmpty()) {
      changeLogBuilder.addAllThreatTypeChanges(
          getWebAppThreatTypeChanges(webAppRulesChangeLog.getThreatTypeChangesList()));
    }

    if (!webAppRulesChangeLog.getRuleChangesList().isEmpty()) {
      changeLogBuilder.addAllRuleChanges(
          getWebAppThreatRuleChanges(webAppRulesChangeLog.getRuleChangesList()));
    }
    return changeLogBuilder.build();
  }

  private RulesChangeLog convertToRuleChangeLog(
      ApiProtectRulesChangeLog apiProtectRulesChangeLog,
      RuleVersion currentVersion,
      RuleVersion previousVersion) {

    RulesChangeLog.Builder changeLogBuilder =
        RulesChangeLog.newBuilder()
            .setVersionUpdated(getRuleVersionUpdateDetails(currentVersion, previousVersion))
            .setRuleType(RuleType.RULE_TYPE_API_PROTECTION);

    if (!apiProtectRulesChangeLog.getThreatTypeChangesList().isEmpty()) {
      changeLogBuilder.addAllThreatTypeChanges(
          getApiProtectThreatTypeChanges(apiProtectRulesChangeLog.getThreatTypeChangesList()));
    }

    if (!apiProtectRulesChangeLog.getRuleChangesList().isEmpty()) {
      changeLogBuilder.addAllRuleChanges(
          getApiProtectThreatRuleChanges(apiProtectRulesChangeLog.getRuleChangesList()));
    }
    return changeLogBuilder.build();
  }

  private void setThreatTypeChanges(
      ThreatTypeChange.Builder builder,
      @Nullable List<String> idsRemoved,
      @Nullable List<String> idsAdded) {
    if (idsRemoved != null && !idsRemoved.isEmpty()) {
      builder.setThreatTypeIdsRemoved(convertToStringList(idsRemoved));
    } else if (idsAdded != null && !idsAdded.isEmpty()) {
      builder.setThreatTypeIdsAdded(convertToStringList(idsAdded));
    }
  }

  private void setRuleChanges(
      ThreatRuleChange.Builder builder,
      @Nullable List<String> idsRemoved,
      @Nullable List<String> idsAdded) {
    if (idsRemoved != null && !idsRemoved.isEmpty()) {
      builder.setRuleIdsRemoved(convertToStringList(idsRemoved));
    } else if (idsAdded != null && !idsAdded.isEmpty()) {
      builder.setRuleIdsAdded(convertToStringList(idsAdded));
    }
  }

  private RuleVersionUpdateDetails.Builder getRuleVersionUpdateDetails(
      RuleVersion currentVersion, RuleVersion previousVersion) {
    return RuleVersionUpdateDetails.newBuilder()
        .setOldVersion(previousVersion)
        .setNewVersion(currentVersion);
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

  private void handleWebAppThreatTypeUpdate(
      ThreatTypeUpdateDetails.ThreatTypeUpdate.Builder builder,
      WebAppThreatTypeUpdateDetails.ThreatTypeUpdate update) {
    switch (update.getUpdateCase()) {
      case NAME_UPDATED:
        builder.setNameUpdated(
            createStringValueUpdate(
                update.getNameUpdated().getOldValue(), update.getNameUpdated().getNewValue()));
        break;
      case THREAT_LABEL_UPDATED:
        builder.setThreatLabelUpdated(processWebAppKeyValueUpdate(update.getThreatLabelUpdated()));
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Invalid change case for ThreatTypeUpdate")
            .asRuntimeException();
    }
  }

  private void handleApiProtectThreatTypeUpdate(
      ThreatTypeUpdateDetails.ThreatTypeUpdate.Builder builder,
      ApiProtectThreatTypeUpdateDetails.ThreatTypeUpdate update) {
    switch (update.getUpdateCase()) {
      case NAME_UPDATED:
        builder.setNameUpdated(
            createStringValueUpdate(
                update.getNameUpdated().getOldValue(), update.getNameUpdated().getNewValue()));
        break;
      case THREAT_LABEL_UPDATED:
        builder.setThreatLabelUpdated(processApiKeyValueUpdate(update.getThreatLabelUpdated()));
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Invalid change case for ThreatTypeUpdate")
            .asRuntimeException();
    }
  }

  private void handleWebAppRuleUpdate(
      ThreatRuleUpdateDetails.ThreatRuleUpdate.Builder builder,
      WebAppThreatRuleUpdateDetails.ThreatRuleUpdate update) {
    switch (update.getUpdateCase()) {
      case NAME_UPDATED:
        builder.setNameUpdated(
            createStringValueUpdate(
                update.getNameUpdated().getOldValue(), update.getNameUpdated().getNewValue()));
        break;
      case THREAT_TYPE_ID_UPDATED:
        builder.setThreatTypeIdUpdated(
            createStringValueUpdate(
                update.getThreatTypeIdUpdated().getOldValue(),
                update.getThreatTypeIdUpdated().getNewValue()));
        break;
      case SIGNATURE_UPDATED:
        builder.setSignatureUpdated(update.getSignatureUpdated());
        break;
      case RULE_TYPE_UPDATED:
        builder.setRuleTypeUpdated(
            createStringValueUpdate(
                update.getRuleTypeUpdated().getOldValue(),
                update.getRuleTypeUpdated().getNewValue()));
        break;
      case SEVERITY_UPDATED:
        builder.setSeverityUpdated(
            createStringValueUpdate(
                update.getSeverityUpdated().getOldValue(),
                update.getSeverityUpdated().getNewValue()));
        break;
      case THREAT_LABEL_UPDATED:
        builder.setThreatLabelUpdated(processWebAppKeyValueUpdate(update.getThreatLabelUpdated()));
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Invalid change case for ThreatRuleUpdate")
            .asRuntimeException();
    }
  }

  private void handleApiProtectRuleUpdate(
      ThreatRuleUpdateDetails.ThreatRuleUpdate.Builder builder,
      ApiProtectThreatRuleUpdateDetails.ThreatRuleUpdate update) {
    switch (update.getUpdateCase()) {
      case NAME_UPDATED:
        builder.setNameUpdated(
            createStringValueUpdate(
                update.getNameUpdated().getOldValue(), update.getNameUpdated().getNewValue()));
        break;
      case THREAT_TYPE_ID_UPDATED:
        builder.setThreatTypeIdUpdated(
            createStringValueUpdate(
                update.getThreatTypeIdUpdated().getOldValue(),
                update.getThreatTypeIdUpdated().getNewValue()));
        break;
      case LOGIC_UPDATED:
        builder.setSignatureUpdated(update.getLogicUpdated());
        break;
      case RULE_TYPE_UPDATED:
        builder.setRuleTypeUpdated(
            createStringValueUpdate(
                update.getRuleTypeUpdated().getOldValue(),
                update.getRuleTypeUpdated().getNewValue()));
        break;
      case SEVERITY_UPDATED:
        builder.setSeverityUpdated(
            createStringValueUpdate(
                update.getSeverityUpdated().getOldValue(),
                update.getSeverityUpdated().getNewValue()));
        break;
      case THREAT_LABEL_UPDATED:
        builder.setThreatLabelUpdated(processApiKeyValueUpdate(update.getThreatLabelUpdated()));
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Invalid change case for ThreatRuleUpdate")
            .asRuntimeException();
    }
  }

  private StringKeyValueUpdate processWebAppKeyValueUpdate(
      ai.traceable.protection.rules.webapp.v1.StringKeyValueUpdate keyValue) {
    StringKeyValueUpdate.Builder builder = createKeyValueUpdateBuilder(keyValue.getKey());
    switch (keyValue.getUpdateCase()) {
      case KEY_REMOVED:
        builder.setKeyRemoved(keyValue.getKeyRemoved());
        break;
      case VALUE_ADDED:
        builder.setValueAdded(keyValue.getValueAdded());
        break;
      case VALUE_UPDATED:
        builder.setValueUpdated(
            createStringValueUpdate(
                keyValue.getValueUpdated().getOldValue(),
                keyValue.getValueUpdated().getNewValue()));
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Invalid change case for StringKeyValueUpdate")
            .asRuntimeException();
    }
    return builder.build();
  }

  private StringKeyValueUpdate processApiKeyValueUpdate(
      ai.traceable.protection.rules.apiprotect.v1.StringKeyValueUpdate keyValue) {
    StringKeyValueUpdate.Builder builder = createKeyValueUpdateBuilder(keyValue.getKey());
    switch (keyValue.getUpdateCase()) {
      case KEY_REMOVED:
        builder.setKeyRemoved(keyValue.getKeyRemoved());
        break;
      case VALUE_ADDED:
        builder.setValueAdded(keyValue.getValueAdded());
        break;
      case VALUE_UPDATED:
        builder.setValueUpdated(
            createStringValueUpdate(
                keyValue.getValueUpdated().getOldValue(),
                keyValue.getValueUpdated().getNewValue()));
        break;
      default:
        throw Status.INVALID_ARGUMENT
            .withDescription("Invalid change case for StringKeyValueUpdate")
            .asRuntimeException();
    }
    return builder.build();
  }

  private StringValueUpdate createStringValueUpdate(String oldValue, String newValue) {
    return StringValueUpdate.newBuilder().setOldValue(oldValue).setNewValue(newValue).build();
  }

  private StringKeyValueUpdate.Builder createKeyValueUpdateBuilder(String key) {
    return StringKeyValueUpdate.newBuilder().setKey(key);
  }

  private StringList convertToStringList(List<String> values) {
    return StringList.newBuilder().addAllValues(values).build();
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

  private List<ThreatTypeChange> getWebAppThreatTypeChanges(
      List<WebAppThreatTypeChange> webAppThreatTypeChanges) {
    List<ThreatTypeChange> threatTypeChanges = new ArrayList<>();
    for (WebAppThreatTypeChange webAppThreatTypeChange : webAppThreatTypeChanges) {

      ThreatTypeChange.Builder threatTypeChangeBuilder = ThreatTypeChange.newBuilder();

      switch (webAppThreatTypeChange.getChangeCase()) {
        case THREAT_TYPE_IDS_REMOVED:
          setThreatTypeChanges(
              threatTypeChangeBuilder,
              webAppThreatTypeChange.getThreatTypeIdsRemoved().getValuesList(),
              null);
          break;
        case THREAT_TYPE_IDS_ADDED:
          setThreatTypeChanges(
              threatTypeChangeBuilder,
              null,
              webAppThreatTypeChange.getThreatTypeIdsAdded().getValuesList());
          break;
        case THREAT_TYPE_UPDATED:
          WebAppThreatTypeUpdateDetails webAppDetails =
              webAppThreatTypeChange.getThreatTypeUpdated();
          ThreatTypeUpdateDetails.Builder detailsBuilder =
              ThreatTypeUpdateDetails.newBuilder().setThreatTypeId(webAppDetails.getThreatTypeId());

          for (WebAppThreatTypeUpdateDetails.ThreatTypeUpdate webAppUpdate :
              webAppDetails.getUpdatesList()) {
            ThreatTypeUpdateDetails.ThreatTypeUpdate.Builder updateBuilder =
                ThreatTypeUpdateDetails.ThreatTypeUpdate.newBuilder();
            handleWebAppThreatTypeUpdate(updateBuilder, webAppUpdate);
            detailsBuilder.addUpdates(updateBuilder.build());
          }
          threatTypeChangeBuilder.setThreatTypeUpdated(detailsBuilder.build());
          break;
        default:
          throw Status.INVALID_ARGUMENT
              .withDescription("Invalid change case for ThreatTypeChange")
              .asRuntimeException();
      }
      threatTypeChanges.add(threatTypeChangeBuilder.build());
    }
    return threatTypeChanges;
  }

  private List<ThreatRuleChange> getWebAppThreatRuleChanges(
      List<WebAppThreatRuleChange> webAppThreatRuleChanges) {
    List<ThreatRuleChange> threatRuleChanges = new ArrayList<>();
    for (WebAppThreatRuleChange webAppThreatRuleChange : webAppThreatRuleChanges) {

      ThreatRuleChange.Builder ruleChangeBuilder = ThreatRuleChange.newBuilder();

      switch (webAppThreatRuleChange.getChangeCase()) {
        case RULE_IDS_REMOVED:
          setRuleChanges(
              ruleChangeBuilder, webAppThreatRuleChange.getRuleIdsRemoved().getValuesList(), null);
          break;
        case RULE_IDS_ADDED:
          setRuleChanges(
              ruleChangeBuilder, null, webAppThreatRuleChange.getRuleIdsAdded().getValuesList());
          break;
        case RULE_UPDATED:
          WebAppThreatRuleUpdateDetails webAppDetails = webAppThreatRuleChange.getRuleUpdated();
          ThreatRuleUpdateDetails.Builder detailsBuilder =
              ThreatRuleUpdateDetails.newBuilder().setRuleId(webAppDetails.getRuleId());

          for (WebAppThreatRuleUpdateDetails.ThreatRuleUpdate webAppUpdate :
              webAppDetails.getUpdatesList()) {
            ThreatRuleUpdateDetails.ThreatRuleUpdate.Builder updateBuilder =
                ThreatRuleUpdateDetails.ThreatRuleUpdate.newBuilder();
            handleWebAppRuleUpdate(updateBuilder, webAppUpdate);
            detailsBuilder.addUpdates(updateBuilder.build());
          }
          ruleChangeBuilder.setRuleUpdated(detailsBuilder.build());
          break;
        default:
          throw Status.INVALID_ARGUMENT
              .withDescription("Invalid change case for ThreatRuleChange")
              .asRuntimeException();
      }
      threatRuleChanges.add(ruleChangeBuilder.build());
    }
    return threatRuleChanges;
  }

  private List<ThreatRuleChange> getApiProtectThreatRuleChanges(
      List<ApiProtectThreatRuleChange> apiProtectThreatRuleChanges) {
    List<ThreatRuleChange> threatRuleChanges = new ArrayList<>();
    for (ApiProtectThreatRuleChange apiProtectThreatRuleChange : apiProtectThreatRuleChanges) {

      ThreatRuleChange.Builder ruleChangeBuilder = ThreatRuleChange.newBuilder();

      switch (apiProtectThreatRuleChange.getChangeCase()) {
        case RULE_IDS_REMOVED:
          setRuleChanges(
              ruleChangeBuilder,
              apiProtectThreatRuleChange.getRuleIdsRemoved().getValuesList(),
              null);
          break;
        case RULE_IDS_ADDED:
          setRuleChanges(
              ruleChangeBuilder,
              null,
              apiProtectThreatRuleChange.getRuleIdsAdded().getValuesList());
          break;
        case RULE_UPDATED:
          ApiProtectThreatRuleUpdateDetails apiDetails =
              apiProtectThreatRuleChange.getRuleUpdated();
          ThreatRuleUpdateDetails.Builder detailsBuilder =
              ThreatRuleUpdateDetails.newBuilder().setRuleId(apiDetails.getRuleId());

          for (ApiProtectThreatRuleUpdateDetails.ThreatRuleUpdate apiUpdate :
              apiDetails.getUpdatesList()) {
            ThreatRuleUpdateDetails.ThreatRuleUpdate.Builder updateBuilder =
                ThreatRuleUpdateDetails.ThreatRuleUpdate.newBuilder();
            handleApiProtectRuleUpdate(updateBuilder, apiUpdate);
            detailsBuilder.addUpdates(updateBuilder.build());
          }
          ruleChangeBuilder.setRuleUpdated(detailsBuilder.build());
          break;
        default:
          throw Status.INVALID_ARGUMENT
              .withDescription("Invalid change case for ThreatRuleChange")
              .asRuntimeException();
      }
      threatRuleChanges.add(ruleChangeBuilder.build());
    }
    return threatRuleChanges;
  }

  private List<ThreatTypeChange> getApiProtectThreatTypeChanges(
      List<ApiProtectThreatTypeChange> apiProtectThreatTypeChanges) {
    List<ThreatTypeChange> threatTypeChanges = new ArrayList<>();
    for (ApiProtectThreatTypeChange apiProtectThreatTypeChange : apiProtectThreatTypeChanges) {

      ThreatTypeChange.Builder threatTypeChangeBuilder = ThreatTypeChange.newBuilder();

      switch (apiProtectThreatTypeChange.getChangeCase()) {
        case THREAT_TYPE_IDS_REMOVED:
          setThreatTypeChanges(
              threatTypeChangeBuilder,
              apiProtectThreatTypeChange.getThreatTypeIdsRemoved().getValuesList(),
              null);
          break;
        case THREAT_TYPE_IDS_ADDED:
          setThreatTypeChanges(
              threatTypeChangeBuilder,
              null,
              apiProtectThreatTypeChange.getThreatTypeIdsAdded().getValuesList());
          break;
        case THREAT_TYPE_UPDATED:
          ApiProtectThreatTypeUpdateDetails apiDetails =
              apiProtectThreatTypeChange.getThreatTypeUpdated();
          ThreatTypeUpdateDetails.Builder detailsBuilder =
              ThreatTypeUpdateDetails.newBuilder().setThreatTypeId(apiDetails.getThreatTypeId());

          for (ApiProtectThreatTypeUpdateDetails.ThreatTypeUpdate apiProtectUpdate :
              apiDetails.getUpdatesList()) {
            ThreatTypeUpdateDetails.ThreatTypeUpdate.Builder updateBuilder =
                ThreatTypeUpdateDetails.ThreatTypeUpdate.newBuilder();
            handleApiProtectThreatTypeUpdate(updateBuilder, apiProtectUpdate);
            detailsBuilder.addUpdates(updateBuilder.build());
          }
          threatTypeChangeBuilder.setThreatTypeUpdated(detailsBuilder.build());
          break;
        default:
          throw Status.INVALID_ARGUMENT
              .withDescription("Invalid change case for ThreatTypeChange")
              .asRuntimeException();
      }
      threatTypeChanges.add(threatTypeChangeBuilder.build());
    }
    return threatTypeChanges;
  }

  private ChangeLog populateChangeLogTables(
      WebAppRulesChangeLog webAppRulesChangeLog,
      WebAppVersionedRules webAppVersionedRule,
      ChangeLog.Builder builder) {

    Map<String, WebAppThreatRule> threatRuleIdToRuleMap =
        webAppVersionedRule.getRulesData().getThreatRulesList().stream()
            .collect(Collectors.toMap(WebAppThreatRule::getThreatRuleId, Function.identity()));

    Map<String, WebAppThreatType> threatTypeIdToRuleMap =
        webAppVersionedRule.getRulesData().getThreatTypesList().stream()
            .collect(Collectors.toMap(WebAppThreatType::getTypeId, Function.identity()));

    Builder addedRuleTableBuilder = getTableBuilder();
    Builder removedRuleTableBuilder = getTableBuilder();
    Builder updatedRuleTableBuilder = getTableBuilder();

    for (WebAppThreatRuleChange threatRuleChange : webAppRulesChangeLog.getRuleChangesList()) {
      ChangeLogRow.Builder rowBuilder = ChangeLogRow.newBuilder();

      switch (threatRuleChange.getChangeCase()) {
        case RULE_IDS_REMOVED:
          for (String id : threatRuleChange.getRuleIdsRemoved().getValuesList()) {
            rowBuilder
                .putValues(
                    THREAT_TYPE,
                    threatTypeIdToRuleMap
                        .get(threatRuleIdToRuleMap.get(id).getRuleDefinition().getThreatTypeId())
                        .getTypeName())
                .putValues(
                    THREAT_RULE, threatRuleIdToRuleMap.get(id).getRuleDefinition().getRuleName())
                .putValues(
                    IS_AGGRESSIVE,
                    threatRuleIdToRuleMap
                            .get(id)
                            .getRuleDefinition()
                            .getRuleType()
                            .equals(WEB_APP_THREAT_RULE_TYPE_AGGRESSIVE)
                        ? "Yes"
                        : "No")
                .putValues(
                    SEVERITY,
                    threatRuleIdToRuleMap
                        .get(id)
                        .getRuleDefinition()
                        .getSeverity()
                        .name()
                        .substring(17));
            removedRuleTableBuilder.addRows(rowBuilder.build());
          }
          break;
        case RULE_IDS_ADDED:
          for (String id : threatRuleChange.getRuleIdsAdded().getValuesList()) {
            rowBuilder
                .putValues(
                    THREAT_TYPE,
                    threatTypeIdToRuleMap
                        .get(threatRuleIdToRuleMap.get(id).getRuleDefinition().getThreatTypeId())
                        .getTypeName())
                .putValues(
                    THREAT_RULE, threatRuleIdToRuleMap.get(id).getRuleDefinition().getRuleName())
                .putValues(
                    IS_AGGRESSIVE,
                    threatRuleIdToRuleMap
                            .get(id)
                            .getRuleDefinition()
                            .getRuleType()
                            .equals(WEB_APP_THREAT_RULE_TYPE_AGGRESSIVE)
                        ? "Yes"
                        : "No")
                .putValues(
                    SEVERITY,
                    threatRuleIdToRuleMap
                        .get(id)
                        .getRuleDefinition()
                        .getSeverity()
                        .name()
                        .substring(17));
            addedRuleTableBuilder.addRows(rowBuilder.build());
          }
          break;
        case RULE_UPDATED:
          WebAppThreatRuleUpdateDetails details = threatRuleChange.getRuleUpdated();
          for (ThreatRuleUpdate threatRuleUpdate : details.getUpdatesList()) {
            if (threatRuleUpdate.hasSignatureUpdated()) {
              rowBuilder
                  .putValues(
                      THREAT_TYPE,
                      threatTypeIdToRuleMap
                          .get(
                              threatRuleIdToRuleMap
                                  .get(details.getRuleId())
                                  .getRuleDefinition()
                                  .getThreatTypeId())
                          .getTypeName())
                  .putValues(
                      THREAT_RULE,
                      threatRuleIdToRuleMap
                          .get(details.getRuleId())
                          .getRuleDefinition()
                          .getRuleName())
                  .putValues(
                      IS_AGGRESSIVE,
                      threatRuleIdToRuleMap
                              .get(details.getRuleId())
                              .getRuleDefinition()
                              .getRuleType()
                              .equals(WEB_APP_THREAT_RULE_TYPE_AGGRESSIVE)
                          ? "Yes"
                          : "No")
                  .putValues(
                      SEVERITY,
                      threatRuleIdToRuleMap
                          .get(details.getRuleId())
                          .getRuleDefinition()
                          .getSeverity()
                          .name()
                          .substring(17));
              updatedRuleTableBuilder.addRows(rowBuilder.build());
            }
          }
          break;
      }
    }

    return builder
        .setAddedRulesTable(addedRuleTableBuilder)
        .setRemovedRulesTable(removedRuleTableBuilder)
        .setUpdatedRulesTable(updatedRuleTableBuilder)
        .build();
  }

  private static Builder getTableBuilder() {
    return ChangeLogTable.newBuilder()
        .addColumnNames(THREAT_TYPE)
        .addColumnNames(THREAT_RULE)
        .addColumnNames(IS_AGGRESSIVE)
        .addColumnNames(SEVERITY);
  }

  private ChangeLog populateChangeLogTables(
      ApiProtectRulesChangeLog apiProtectRulesChangeLog,
      ApiProtectVersionedRules apiProtectVersionedRules,
      ChangeLog.Builder builder) {

    Map<String, ApiProtectThreatRule> threatRuleIdToRuleMap =
        apiProtectVersionedRules.getRulesData().getThreatRulesList().stream()
            .collect(Collectors.toMap(ApiProtectThreatRule::getRuleId, Function.identity()));

    Map<String, ApiProtectThreatType> threatTypeIdToRuleMap =
        apiProtectVersionedRules.getRulesData().getThreatTypesList().stream()
            .collect(Collectors.toMap(ApiProtectThreatType::getTypeId, Function.identity()));

    Builder addedRuleTableBuilder = getTableBuilder();
    Builder removedRuleTableBuilder = getTableBuilder();
    Builder updatedRuleTableBuilder = getTableBuilder();

    for (ApiProtectThreatRuleChange threatRuleChange :
        apiProtectRulesChangeLog.getRuleChangesList()) {
      ChangeLogRow.Builder rowBuilder = ChangeLogRow.newBuilder();

      switch (threatRuleChange.getChangeCase()) {
        case RULE_IDS_REMOVED:
          for (String id : threatRuleChange.getRuleIdsRemoved().getValuesList()) {
            rowBuilder
                .putValues(
                    THREAT_TYPE,
                    threatTypeIdToRuleMap
                        .get(threatRuleIdToRuleMap.get(id).getRuleDefinition().getThreatTypeId())
                        .getTypeName())
                .putValues(
                    THREAT_RULE, threatRuleIdToRuleMap.get(id).getRuleDefinition().getRuleName())
                .putValues(
                    IS_STANDARD,
                    threatRuleIdToRuleMap
                            .get(id)
                            .getRuleDefinition()
                            .getRuleType()
                            .equals(ApiProtectThreatRuleType.API_PROTECT_THREAT_RULE_TYPE_STANDARD)
                        ? "Yes"
                        : "No")
                .putValues(
                    SEVERITY,
                    threatRuleIdToRuleMap
                        .get(id)
                        .getRuleDefinition()
                        .getSeverity()
                        .name()
                        .substring(21));
            removedRuleTableBuilder.addRows(rowBuilder.build());
          }
          break;
        case RULE_IDS_ADDED:
          for (String id : threatRuleChange.getRuleIdsAdded().getValuesList()) {
            rowBuilder
                .putValues(
                    THREAT_TYPE,
                    threatTypeIdToRuleMap
                        .get(threatRuleIdToRuleMap.get(id).getRuleDefinition().getThreatTypeId())
                        .getTypeName())
                .putValues(
                    THREAT_RULE, threatRuleIdToRuleMap.get(id).getRuleDefinition().getRuleName())
                .putValues(
                    IS_STANDARD,
                    threatRuleIdToRuleMap
                            .get(id)
                            .getRuleDefinition()
                            .getRuleType()
                            .equals(ApiProtectThreatRuleType.API_PROTECT_THREAT_RULE_TYPE_STANDARD)
                        ? "Yes"
                        : "No")
                .putValues(
                    SEVERITY,
                    threatRuleIdToRuleMap
                        .get(id)
                        .getRuleDefinition()
                        .getSeverity()
                        .name()
                        .substring(21));
            addedRuleTableBuilder.addRows(rowBuilder.build());
          }
          break;
        case RULE_UPDATED:
          ApiProtectThreatRuleUpdateDetails details = threatRuleChange.getRuleUpdated();
          for (ai.traceable.protection.rules.apiprotect.v1.ApiProtectThreatRuleUpdateDetails
                  .ThreatRuleUpdate
              threatRuleUpdate : details.getUpdatesList()) {
            if (threatRuleUpdate.hasLogicUpdated()) {
              rowBuilder
                  .putValues(
                      THREAT_TYPE,
                      threatTypeIdToRuleMap
                          .get(
                              threatRuleIdToRuleMap
                                  .get(details.getRuleId())
                                  .getRuleDefinition()
                                  .getThreatTypeId())
                          .getTypeName())
                  .putValues(
                      THREAT_RULE,
                      threatRuleIdToRuleMap
                          .get(details.getRuleId())
                          .getRuleDefinition()
                          .getRuleName())
                  .putValues(
                      IS_STANDARD,
                      threatRuleIdToRuleMap
                              .get(details.getRuleId())
                              .getRuleDefinition()
                              .getRuleType()
                              .equals(
                                  ApiProtectThreatRuleType.API_PROTECT_THREAT_RULE_TYPE_STANDARD)
                          ? "Yes"
                          : "No")
                  .putValues(
                      SEVERITY,
                      threatRuleIdToRuleMap
                          .get(details.getRuleId())
                          .getRuleDefinition()
                          .getSeverity()
                          .name()
                          .substring(21));
              updatedRuleTableBuilder.addRows(rowBuilder.build());
            }
          }
          break;
      }
    }

    return builder
        .setAddedRulesTable(addedRuleTableBuilder)
        .setRemovedRulesTable(removedRuleTableBuilder)
        .setUpdatedRulesTable(updatedRuleTableBuilder)
        .build();
  }

  private StringList getCurrentVersionHighlights(String highlights) {
    if (highlights == null || highlights.isEmpty()) {
      return StringList.getDefaultInstance();
    }
    return StringList.newBuilder().addAllValues(List.of(highlights.split("\\n"))).build();
  }
}
