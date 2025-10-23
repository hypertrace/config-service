package ai.traceable.anomaly.config.service.detector.migration;

import static ai.traceable.anomaly.config.service.v1.ApiProtectThreatRuleConfigMappingProvider.GQLA_THREAT_TYPE_ID;
import static ai.traceable.anomaly.config.service.v1.ApiProtectThreatRuleConfigMappingProvider.JWT_THREAT_TYPE_ID;
import static ai.traceable.anomaly.config.service.v1.ApiProtectThreatRuleConfigMappingProvider.SESSIONV_THREAT_TYPE_ID;

import ai.traceable.anomaly.config.service.v1.AnomalyRuleAction;
import ai.traceable.anomaly.config.service.v1.ApiProtectThreatRuleConfigMappingProvider;
import ai.traceable.anomaly.config.service.v1.StringList;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyCategoryConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.AnomalySubRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiProtectAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.ApiProtectAnomalyRuleConfig;
import ai.traceable.anomaly.config.service.v1.detector.ConfigMetadata;
import ai.traceable.anomaly.config.service.v1.detector.IpTypeList;
import ai.traceable.anomaly.config.service.v1.detector.MultiValuedStringParamRule;
import ai.traceable.anomaly.config.service.v1.detector.MultiValuedStringParamRulesList;
import ai.traceable.anomaly.config.service.v1.detector.ScopedAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.SessionDefinitionMetadataAnomalyDetectionConfig;
import ai.traceable.anomaly.config.service.v1.detector.UserIdData;
import ai.traceable.anomaly.config.service.v1.detector.UserIdDataList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.ListValue;
import com.google.protobuf.Message;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import jakarta.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Singleton
public class ApiProtectMigrationProcessor {

  private static final ImmutableMap<String, Map<String, List<String>>>
      OLD_TYPE_ID_TO_NEW_THREAT_TYPE_ID_WITH_SUB_RULES_ID_MAPPING =
          ApiProtectMigrationMappings.initOldTypeIdToNewTypeIdWithSubRulesIdMap();
  public static final String UNSPECIFIED = "_UNSPECIFIED";

  public ScopedAnomalyDetectionConfig migrateConfig(ScopedAnomalyDetectionConfig config) {
    if (config.getAnomalyDetectionConfigsList().isEmpty() || hasApiProtectConfig(config)) {
      return config;
    }

    Map<String, List<AnomalySubRuleConfig>> newThreatTypeIdToRuleConfigs = new HashMap<>();
    Map<String, AnomalyDetectionConfig> threatTypeIdToSourceConfig = new HashMap<>();
    for (AnomalyDetectionConfig detectionConfig : config.getAnomalyDetectionConfigsList()) {
      processAnomalyDetectionConfig(
          detectionConfig, newThreatTypeIdToRuleConfigs, threatTypeIdToSourceConfig);
    }

    if (newThreatTypeIdToRuleConfigs.isEmpty()) {
      return config;
    }
    ScopedAnomalyDetectionConfig.Builder updatedConfig =
        ScopedAnomalyDetectionConfig.newBuilder(config);
    for (Map.Entry<String, List<AnomalySubRuleConfig>> entry :
        newThreatTypeIdToRuleConfigs.entrySet()) {
      String threatTypeId = entry.getKey();
      List<AnomalySubRuleConfig> subRules = entry.getValue();
      AnomalyDetectionConfig sourceConfig = threatTypeIdToSourceConfig.get(threatTypeId);
      addNewApiProtectEntry(updatedConfig, threatTypeId, subRules, sourceConfig);
    }

    return updatedConfig.build();
  }

  private void processAnomalyDetectionConfig(
      AnomalyDetectionConfig detectionConfig,
      Map<String, List<AnomalySubRuleConfig>> newThreatTypeIdToRuleConfigs,
      Map<String, AnomalyDetectionConfig> threatTypeIdToSourceConfig) {

    switch (detectionConfig.getAnomalyDetectionConfigCase()) {
      case API_DEFINITION_METADATA_ANOMALY_DETECTION_CONFIG:
        processApiDefinitionMetadataConfig(
            detectionConfig,
            detectionConfig.getApiDefinitionMetadataAnomalyDetectionConfig(),
            newThreatTypeIdToRuleConfigs,
            threatTypeIdToSourceConfig);
        break;

      case SESSION_DEFINITION_METADATA_ANOMALY_DETECTION_CONFIG:
        processSessionDefinitionMetadataConfig(
            detectionConfig,
            detectionConfig.getSessionDefinitionMetadataAnomalyDetectionConfig(),
            newThreatTypeIdToRuleConfigs,
            threatTypeIdToSourceConfig);
        break;
      default:
    }
  }

  private void processApiDefinitionMetadataConfig(
      AnomalyDetectionConfig detectionConfig,
      ApiDefinitionMetadataAnomalyDetectionConfig ruleConfig,
      Map<String, List<AnomalySubRuleConfig>> newThreatTypeIdToRuleConfigs,
      Map<String, AnomalyDetectionConfig> threatTypeIdToSourceConfig) {

    String oldThreatTypeId = ruleConfig.getAnomalyRuleId();
    if (!OLD_TYPE_ID_TO_NEW_THREAT_TYPE_ID_WITH_SUB_RULES_ID_MAPPING.containsKey(oldThreatTypeId)) {
      return;
    }
    Map<String, Value> familyParams = extractFamilyParams(detectionConfig);
    Map<String, AnomalySubRuleConfig> sourceSubRules = extractSubRules(ruleConfig);
    processConfigsForOldThreatType(
        detectionConfig,
        oldThreatTypeId,
        familyParams,
        sourceSubRules,
        newThreatTypeIdToRuleConfigs,
        threatTypeIdToSourceConfig);
  }

  private void processSessionDefinitionMetadataConfig(
      AnomalyDetectionConfig detectionConfig,
      SessionDefinitionMetadataAnomalyDetectionConfig ruleConfig,
      Map<String, List<AnomalySubRuleConfig>> newThreatTypeIdToRuleConfigs,
      Map<String, AnomalyDetectionConfig> threatTypeIdToSourceConfig) {

    String oldThreatTypeId = ruleConfig.getAnomalyRuleId();
    if (!OLD_TYPE_ID_TO_NEW_THREAT_TYPE_ID_WITH_SUB_RULES_ID_MAPPING.containsKey(oldThreatTypeId)) {
      return;
    }
    Map<String, Value> familyParams = extractFamilyParams(detectionConfig);
    Map<String, AnomalySubRuleConfig> sourceSubRules = extractSessionSubRules(ruleConfig);
    processConfigsForOldThreatType(
        detectionConfig,
        oldThreatTypeId,
        familyParams,
        sourceSubRules,
        newThreatTypeIdToRuleConfigs,
        threatTypeIdToSourceConfig);
  }

  private void processConfigsForOldThreatType(
      AnomalyDetectionConfig detectionConfig,
      String oldThreatTypeId,
      Map<String, Value> familyParams,
      Map<String, AnomalySubRuleConfig> sourceSubRules,
      Map<String, List<AnomalySubRuleConfig>> newThreatTypeIdToRuleConfigs,
      Map<String, AnomalyDetectionConfig> threatTypeIdToSourceConfig) {

    Map<String, List<String>> oldTypeIdToNewTypeIdWithSubRulesIdMap =
        OLD_TYPE_ID_TO_NEW_THREAT_TYPE_ID_WITH_SUB_RULES_ID_MAPPING.get(oldThreatTypeId);
    if (oldTypeIdToNewTypeIdWithSubRulesIdMap == null) {
      return;
    }
    for (Map.Entry<String, List<String>> entry : oldTypeIdToNewTypeIdWithSubRulesIdMap.entrySet()) {
      String newThreatTypeId = entry.getKey();
      List<String> newRuleIds = entry.getValue();
      if (oldThreatTypeId.equals(newThreatTypeId)) {
        threatTypeIdToSourceConfig.put(newThreatTypeId, detectionConfig);
      }
      if (!newThreatTypeIdToRuleConfigs.containsKey(newThreatTypeId)) {
        newThreatTypeIdToRuleConfigs.put(newThreatTypeId, new ArrayList<>());
      }
      for (String newRuleId : newRuleIds) {
        Map<String, Value> configParams = new HashMap<>();

        List<ConfigMetadata> configMetadataList =
            ApiProtectThreatRuleConfigMappingProvider.getConfigMetadata(newRuleId);

        if (configMetadataList != null && !configMetadataList.isEmpty()) {
          List<String> fields =
              configMetadataList.stream().map(ConfigMetadata::getKey).collect(Collectors.toList());
          for (String field : fields) {
            if (familyParams.containsKey(field)) {
              configParams.put(field, familyParams.get(field));
            }
          }
        }

        AnomalySubRuleConfig sourceSub = sourceSubRules.get(newRuleId);
        AnomalySubRuleConfig.Builder newSubBuilder =
            AnomalySubRuleConfig.newBuilder().setSubRuleId(newRuleId);
        getAnomalyRuleAction(detectionConfig, sourceSub)
            .ifPresent(newSubBuilder::setAnomalyRuleAction);
        newSubBuilder.putAllConfigParams(configParams);
        if (sourceSub == null) {
          newThreatTypeIdToRuleConfigs.get(newThreatTypeId).add(newSubBuilder.build());
          continue;
        }
        if (sourceSub.hasCategoryConfig()) {
          newSubBuilder.setCategoryConfig(sourceSub.getCategoryConfig());
        }
        if (sourceSub.hasInternal()) {
          newSubBuilder.setInternal(sourceSub.getInternal());
        }
        newThreatTypeIdToRuleConfigs.get(newThreatTypeId).add(newSubBuilder.build());
      }
    }
  }

  private Optional<AnomalyRuleAction> getAnomalyRuleAction(
      AnomalyDetectionConfig detectionConfig, AnomalySubRuleConfig sourceSub) {
    if (sourceSub == null && mapsToThreatTypeWithExplicitMultipleThreatRulesInV1(detectionConfig)) {
      return Optional.empty();
    } else if (sourceSub != null) {
      if (sourceSub.getAnomalyRuleAction() != AnomalyRuleAction.ANOMALY_RULE_ACTION_UNSPECIFIED) {
        return Optional.of(sourceSub.getAnomalyRuleAction());
      } else if (sourceSub.hasConfigStatus()) {
        boolean isDisabled = sourceSub.getConfigStatus().getDisabled();
        return isDisabled
            ? Optional.of(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE)
            : Optional.of(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR);
      }
    }
    if (detectionConfig.hasConfigStatus()) {
      boolean isDisabled = detectionConfig.getConfigStatus().getDisabled();
      return isDisabled
          ? Optional.of(AnomalyRuleAction.ANOMALY_RULE_ACTION_DISABLE)
          : Optional.of(AnomalyRuleAction.ANOMALY_RULE_ACTION_MONITOR);
    }
    return Optional.empty();
  }

  private Map<String, Value> extractFamilyParams(AnomalyDetectionConfig config) {
    Map<String, Value> params = new HashMap<>();
    switch (config.getAnomalyDetectionConfigCase()) {
      case API_DEFINITION_METADATA_ANOMALY_DETECTION_CONFIG:
        extractParamsFromApiDefinitionConfig(
            config.getApiDefinitionMetadataAnomalyDetectionConfig(), params);
        break;
      case SESSION_DEFINITION_METADATA_ANOMALY_DETECTION_CONFIG:
        extractParamsFromSessionDefinitionConfig(
            config.getSessionDefinitionMetadataAnomalyDetectionConfig(), params);
        break;
      default:
    }

    return params;
  }

  private void extractParamsFromApiDefinitionConfig(
      ApiDefinitionMetadataAnomalyDetectionConfig config, Map<String, Value> params) {
    Message specificConfig = getSpecificConfigFromApiDefinition(config);
    if (specificConfig != null) {
      extractParamsFromMessage(specificConfig, params);
    }
  }

  private void extractParamsFromSessionDefinitionConfig(
      SessionDefinitionMetadataAnomalyDetectionConfig config, Map<String, Value> params) {
    Message specificConfig = getSpecificConfigFromSessionDefinition(config);
    if (specificConfig != null) {
      extractParamsFromMessage(specificConfig, params);
    }
  }

  private Message getSpecificConfigFromApiDefinition(
      ApiDefinitionMetadataAnomalyDetectionConfig config) {
    switch (config.getConfigCase()) {
      case JWT:
        return config.getJwt();
      case CONTENT_TYPE:
        return config.getContentType();
      case MISSING_PARAM:
        return config.getMissingParam();
      case BFLA:
        return config.getBfla();
      case ENUM:
        return config.getEnum();
      case SSRF:
        return config.getSsrf();
      case TYPE:
        return config.getType();
      case DEVICE:
        return config.getDevice();
      case INTEGER:
        return config.getInteger();
      case PAYLOAD:
        return config.getPayload();
      case HTTP_STATUS:
        return config.getHttpStatus();
      case UNKNOWN_PARAM:
        return config.getUnknownParam();
      case CONTENT_SIZE:
        return config.getContentSize();
      case CONTENT_EXPLOSION:
        return config.getContentExplosion();
      case SPECIAL_CHARACTER:
        return config.getSpecialCharacter();
      case GQLA:
        return config.getGqla();
      default:
        return null;
    }
  }

  private Message getSpecificConfigFromSessionDefinition(
      SessionDefinitionMetadataAnomalyDetectionConfig config) {
    switch (config.getConfigCase()) {
      case SESSION_VIOLATION:
        return config.getSessionViolation();
      case OBJECT_BOLA:
        return config.getObjectBola();
      case USER_ID_BOLA:
        return config.getUserIdBola();
      case SEQUENCE_ANOMALY:
        return config.getSequenceAnomaly();
      default:
        return null;
    }
  }

  private void extractParamsFromMessage(Message message, Map<String, Value> params) {
    if (message == null) {
      return;
    }

    Descriptor descriptor = message.getDescriptorForType();
    for (FieldDescriptor fieldDescriptor : descriptor.getFields()) {
      // we don't have map fields in config protos
      if (fieldDescriptor.isMapField()) {
        continue;
      }
      // clear fields are not set
      if (fieldDescriptor.isRepeated()) {
        Object fieldValue = message.getField(fieldDescriptor);
        if (fieldValue instanceof List && ((List<?>) fieldValue).isEmpty()) {
          continue;
        }
      } else {
        if (!message.hasField(fieldDescriptor)) {
          continue;
        }
      }

      String fieldName = fieldDescriptor.getName();
      Object fieldValue = message.getField(fieldDescriptor);

      if (fieldDescriptor.getJavaType() == FieldDescriptor.JavaType.MESSAGE
          && !(fieldValue instanceof UserIdDataList)
          && !(fieldValue instanceof MultiValuedStringParamRulesList)
          && !(fieldValue instanceof IpTypeList)
          && !(fieldValue instanceof StringList)) {
        extractParamsFromMessage((Message) fieldValue, params);
      } else {

        Value value = convertFieldToValue(fieldDescriptor, fieldValue);
        if (value != null) {
          params.put(fieldName, value);
        }
      }
    }
  }

  private Value convertFieldToValue(FieldDescriptor fieldDescriptor, Object fieldValue) {
    if (fieldValue == null) {
      return null;
    }
    if (fieldDescriptor.isRepeated()) {
      return convertRepeatedFieldToValue(fieldDescriptor, (List<?>) fieldValue);
    }

    switch (fieldDescriptor.getJavaType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return Value.newBuilder().setNumberValue(((Number) fieldValue).doubleValue()).build();
      case BOOLEAN:
        return Value.newBuilder().setBoolValue((Boolean) fieldValue).build();
      case STRING:
        return Value.newBuilder().setStringValue((String) fieldValue).build();
      case ENUM:
        if (fieldValue.toString().endsWith(UNSPECIFIED)) {
          return null;
        }
        return Value.newBuilder().setStringValue(fieldValue.toString()).build();
      case MESSAGE:
        if (fieldValue instanceof StringList) {
          StringList stringList = (StringList) fieldValue;
          ListValue.Builder listBuilder = ListValue.newBuilder();
          for (String item : stringList.getValuesList()) {
            listBuilder.addValues(Value.newBuilder().setStringValue(item).build());
          }
          return Value.newBuilder().setListValue(listBuilder.build()).build();
        } else if (fieldValue instanceof MultiValuedStringParamRulesList) {
          MultiValuedStringParamRulesList rulesList = (MultiValuedStringParamRulesList) fieldValue;
          ListValue.Builder listBuilder = ListValue.newBuilder();
          for (MultiValuedStringParamRule rule : rulesList.getRulesList()) {
            Struct struct = Struct.newBuilder().putAllFields(convertToMap(rule)).build();
            listBuilder.addValues(Value.newBuilder().setStructValue(struct).build());
          }
          return Value.newBuilder().setListValue(listBuilder.build()).build();
        } else if (fieldValue instanceof UserIdDataList) {
          UserIdDataList userIdDataList = (UserIdDataList) fieldValue;
          ListValue.Builder listBuilder = ListValue.newBuilder();
          for (UserIdData userIdData : userIdDataList.getUserIdDataList()) {
            Struct struct = convertUserIdDataToStruct(userIdData);
            listBuilder.addValues(Value.newBuilder().setStructValue(struct).build());
          }
          return Value.newBuilder().setListValue(listBuilder.build()).build();
        } else if (fieldValue instanceof IpTypeList) {
          IpTypeList ipTypeList = (IpTypeList) fieldValue;
          return convertFieldToValue(
              fieldDescriptor.getMessageType().findFieldByNumber(IpTypeList.VALUES_FIELD_NUMBER),
              ipTypeList.getValuesList());
        }
        return null;
      default:
        return null;
    }
  }

  private Value convertRepeatedFieldToValue(FieldDescriptor fieldDescriptor, List<?> list) {
    if (list.isEmpty()) {
      return null;
    }

    ListValue.Builder listBuilder = ListValue.newBuilder();
    for (Object item : list) {
      Value itemValue;
      switch (fieldDescriptor.getJavaType()) {
        case INT:
        case LONG:
        case FLOAT:
        case DOUBLE:
          itemValue = Value.newBuilder().setNumberValue(((Number) item).doubleValue()).build();
          break;
        case STRING:
          itemValue = Value.newBuilder().setStringValue((String) item).build();
          break;
        case ENUM:
          itemValue = Value.newBuilder().setStringValue(item.toString()).build();
          break;
        case MESSAGE:
          if (item instanceof Message) {
            Message nestedMessage = (Message) item;
            Map<String, Value> nestedParams = new HashMap<>();
            extractParamsFromMessage(nestedMessage, nestedParams);
            if (!nestedParams.isEmpty()) {
              itemValue =
                  Value.newBuilder()
                      .setStructValue(Struct.newBuilder().putAllFields(nestedParams).build())
                      .build();
            } else {
              continue;
            }
          } else {
            continue;
          }
          break;
        default:
          continue;
      }
      listBuilder.addValues(itemValue);
    }

    return listBuilder.getValuesCount() > 0
        ? Value.newBuilder().setListValue(listBuilder.build()).build()
        : null;
  }

  private Map<String, AnomalySubRuleConfig> extractSubRules(
      ApiDefinitionMetadataAnomalyDetectionConfig config) {
    Map<String, AnomalySubRuleConfig> result = new HashMap<>();
    if (config.hasSubRuleConfigs()) {
      result.putAll(config.getSubRuleConfigs().getSubRuleConfigsMap());
    }
    return result;
  }

  private Map<String, AnomalySubRuleConfig> extractSessionSubRules(
      SessionDefinitionMetadataAnomalyDetectionConfig config) {
    Map<String, AnomalySubRuleConfig> result = new HashMap<>();
    if (config.hasSubRuleConfigs()) {
      result.putAll(config.getSubRuleConfigs().getSubRuleConfigsMap());
    }
    return result;
  }

  private void addNewApiProtectEntry(
      ScopedAnomalyDetectionConfig.Builder configBuilder,
      String threatTypeId,
      List<AnomalySubRuleConfig> subRules,
      AnomalyDetectionConfig sourceConfig) {

    ApiProtectAnomalyRuleConfig.Builder newRule =
        ApiProtectAnomalyRuleConfig.newBuilder().setAnomalyRuleId(threatTypeId);
    for (AnomalySubRuleConfig sub : subRules) {
      newRule.addSubRuleConfigs(sub);
    }

    AnomalyDetectionConfig.Builder newConfigBuilder = AnomalyDetectionConfig.newBuilder();
    if (sourceConfig != null) {
      if (sourceConfig.hasCategoryConfig()) {
        newConfigBuilder.setCategoryConfig(sourceConfig.getCategoryConfig());
      }
      if (sourceConfig.hasConfigStatus()) {
        newConfigBuilder.setConfigStatus(sourceConfig.getConfigStatus());
      }
    } else {
      newConfigBuilder.setCategoryConfig(AnomalyCategoryConfig.getDefaultInstance());
    }
    newConfigBuilder.setApiProtectAnomalyDetectionConfig(
        ApiProtectAnomalyDetectionConfig.newBuilder().setApiProtectAnomalyRule(newRule));

    configBuilder.addAnomalyDetectionConfigs(newConfigBuilder.build());
  }

  private boolean hasApiProtectConfig(ScopedAnomalyDetectionConfig config) {
    for (AnomalyDetectionConfig detectionConfig : config.getAnomalyDetectionConfigsList()) {
      if (detectionConfig.hasApiProtectAnomalyDetectionConfig()) {
        return true;
      }
    }
    return false;
  }

  private boolean mapsToThreatTypeWithExplicitMultipleThreatRulesInV1(
      AnomalyDetectionConfig config) {
    if (config == null) {
      return false;
    }
    if (config.hasApiDefinitionMetadataAnomalyDetectionConfig()) {
      ApiDefinitionMetadataAnomalyDetectionConfig apiConfig =
          config.getApiDefinitionMetadataAnomalyDetectionConfig();
      return apiConfig.getAnomalyRuleId().equals(JWT_THREAT_TYPE_ID)
          || apiConfig.getAnomalyRuleId().equals(GQLA_THREAT_TYPE_ID);
    } else if (config.hasSessionDefinitionMetadataAnomalyDetectionConfig()) {
      SessionDefinitionMetadataAnomalyDetectionConfig sessionConfig =
          config.getSessionDefinitionMetadataAnomalyDetectionConfig();
      return sessionConfig.getAnomalyRuleId().equals(SESSIONV_THREAT_TYPE_ID);
    }
    return false;
  }

  private Struct convertUserIdDataToStruct(Message message) {
    Map<String, Value> fields = new HashMap<>();
    extractParamsFromMessage(message, fields);
    return Struct.newBuilder().putAllFields(fields).build();
  }

  private Map<String, Value> convertToMap(MultiValuedStringParamRule multiValuedStringParamRule) {
    Map<String, Value> map = new HashMap<>();
    extractParamsFromMessage(multiValuedStringParamRule, map);
    return map;
  }
}
