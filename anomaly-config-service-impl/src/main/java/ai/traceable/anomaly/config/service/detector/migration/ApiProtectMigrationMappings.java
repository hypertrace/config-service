package ai.traceable.anomaly.config.service.detector.migration;

import ai.traceable.anomaly.config.service.v1.ApiProtectThreatRuleConfigMappingProvider;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ApiProtectMigrationMappings {
  static ImmutableMap<String, Map<String, List<String>>>
      initOldTypeIdToNewTypeIdWithSubRulesIdMap() {
    return getOldTypeIdToNewTypeIdWithSubRulesIdMap();
  }

  private static ImmutableMap<String, Map<String, List<String>>>
      getOldTypeIdToNewTypeIdWithSubRulesIdMap() {
    ImmutableMap.Builder<String, Map<String, List<String>>> mapBuilder = ImmutableMap.builder();

    // JWT mappings
    Map<String, List<String>> jwtMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.JWT_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.JWT_EXP_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.JWT_NBF_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.JWT_ISS_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.JWT_AUD_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.JWT_ALG_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.JWT_SIGN_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.JWT_ALBEAST_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.JWT_JKU_SUB_RULE_ID));
    mapBuilder.put(ApiProtectThreatRuleConfigMappingProvider.JWT_THREAT_TYPE_ID, jwtMap);

    // GQLA mappings
    Map<String, List<String>> gqlaMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.GQLA_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.GQLA_ABA_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.GQLA_DRQ_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.GQLA_FDA_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.GQLA_BQA_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.GQLA_CF_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.GQLA_IQ_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.GQLA_RIQ_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.GQLA_FS_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.GQLA_GST_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.GQLA_GDLB_SUB_RULE_ID));
    mapBuilder.put(ApiProtectThreatRuleConfigMappingProvider.GQLA_THREAT_TYPE_ID, gqlaMap);

    // BOLA mappings
    Map<String, List<String>> bolaMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.AUTHZ_THREAT_TYPE_ID,
            ImmutableList.of(ApiProtectThreatRuleConfigMappingProvider.AUTHZ_BOLA_SUB_RULE_ID));
    mapBuilder.put(ApiProtectThreatRuleConfigMappingProvider.BOLA_THREAT_TYPE_ID, bolaMap);

    // UserIdBOLA mappings
    Map<String, List<String>> userIdBolaMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.AUTHZ_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.AUTHZ_USER_ID_BOLA_SUB_RULE_ID));
    mapBuilder.put(
        ApiProtectThreatRuleConfigMappingProvider.USER_ID_BOLA_THREAT_TYPE_ID, userIdBolaMap);

    // BFLA mappings
    Map<String, List<String>> bflaMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.AUTHZ_THREAT_TYPE_ID,
            ImmutableList.of(ApiProtectThreatRuleConfigMappingProvider.AUTHZ_BFLA_SUB_RULE_ID));
    mapBuilder.put(ApiProtectThreatRuleConfigMappingProvider.BFLA_THREAT_TYPE_ID, bflaMap);

    // Missing Param mappings
    Map<String, List<String>> missingParamMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.AUTHZ_THREAT_TYPE_ID,
            ImmutableList.of(ApiProtectThreatRuleConfigMappingProvider.AUTHZ_CSRF_SUB_RULE_ID),
            ApiProtectThreatRuleConfigMappingProvider.AUTHN_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.AUTHN_AUA_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.AUTHN_UA_SUB_RULE_ID),
            ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_MREQP_SUB_RULE_ID));
    mapBuilder.put(
        ApiProtectThreatRuleConfigMappingProvider.MISSING_PARAM_THREAT_TYPE_ID, missingParamMap);

    // SessionV mappings
    Map<String, List<String>> sessionvMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.SESSIONV_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.SESSIONV_LANDSPEED_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.SESSIONV_EXPIRY_SUB_RULE_ID));
    mapBuilder.put(ApiProtectThreatRuleConfigMappingProvider.SESSIONV_THREAT_TYPE_ID, sessionvMap);

    // SSRF mappings
    Map<String, List<String>> ssrfMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.SSRF_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.SSRF_UH_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.SSRF_UP_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.SSRF_MH_SUB_RULE_ID));
    mapBuilder.put(ApiProtectThreatRuleConfigMappingProvider.SSRF_THREAT_TYPE_ID, ssrfMap);

    // Content Size mappings
    Map<String, List<String>> contentSizeMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.CONTENT_ANOMALY_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.CONTENT_ANOMALY_UREQCL_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.CONTENT_ANOMALY_URESCL_SUB_RULE_ID));
    mapBuilder.put(
        ApiProtectThreatRuleConfigMappingProvider.CONTENT_SIZE_THREAT_TYPE_ID, contentSizeMap);

    // Content Type mappings
    Map<String, List<String>> contentTypeMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.CONTENT_ANOMALY_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.CONTENT_ANOMALY_REQCTM_SUB_RULE_ID),
            ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_REQCTVE_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_RESCTVE_SUB_RULE_ID));
    mapBuilder.put(
        ApiProtectThreatRuleConfigMappingProvider.CONTENT_TYPE_THREAT_TYPE_ID, contentTypeMap);

    // Content Explosion mappings
    Map<String, List<String>> contentExplosionMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.CONTENT_ANOMALY_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.CONTENT_ANOMALY_REQCE_SUB_RULE_ID));
    mapBuilder.put(
        ApiProtectThreatRuleConfigMappingProvider.CONTENT_EXPLOSION_THREAT_TYPE_ID,
        contentExplosionMap);

    // Special Character mappings
    Map<String, List<String>> specialCharacterMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_SCT_SUB_RULE_ID));
    mapBuilder.put(
        ApiProtectThreatRuleConfigMappingProvider.SPECIAL_CHARACTER_THREAT_TYPE_ID,
        specialCharacterMap);

    // Integer mappings
    Map<String, List<String>> integerMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_INTVOR_SUB_RULE_ID,
                ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_INTUNS_SUB_RULE_ID));
    mapBuilder.put(ApiProtectThreatRuleConfigMappingProvider.INTEGER_THREAT_TYPE_ID, integerMap);

    // Device mappings
    Map<String, List<String>> deviceMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.PARAMETER_ANOMALY_UUAD_SUB_RULE_ID));
    mapBuilder.put(ApiProtectThreatRuleConfigMappingProvider.DEVICE_THREAT_TYPE_ID, deviceMap);

    // Enum mappings
    Map<String, List<String>> enumMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_REQIE_SUB_RULE_ID));
    mapBuilder.put(ApiProtectThreatRuleConfigMappingProvider.ENUM_THREAT_TYPE_ID, enumMap);

    // Unknown Param mappings
    Map<String, List<String>> unknownParamMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_UREQP_SUB_RULE_ID));
    mapBuilder.put(
        ApiProtectThreatRuleConfigMappingProvider.UNKNOWN_PARAM_THREAT_TYPE_ID, unknownParamMap);

    // Type mappings
    Map<String, List<String>> typeMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_REQPTVE_SUB_RULE_ID));
    mapBuilder.put(ApiProtectThreatRuleConfigMappingProvider.TYPE_THREAT_TYPE_ID, typeMap);

    // HTTP Status mappings
    Map<String, List<String>> httpStatusMap =
        ImmutableMap.of(
            ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_THREAT_TYPE_ID,
            ImmutableList.of(
                ApiProtectThreatRuleConfigMappingProvider.SCHEMA_VALIDATION_URESC_SUB_RULE_ID));
    mapBuilder.put(
        ApiProtectThreatRuleConfigMappingProvider.HTTP_STATUS_THREAT_TYPE_ID, httpStatusMap);

    return mapBuilder.build();
  }
}
