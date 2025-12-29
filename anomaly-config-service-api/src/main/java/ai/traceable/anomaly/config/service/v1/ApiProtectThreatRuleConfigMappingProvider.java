package ai.traceable.anomaly.config.service.v1;

import ai.traceable.anomaly.config.service.v1.detector.ConfigMetadata;
import ai.traceable.anomaly.config.service.v1.detector.ConfigValueMetadata;
import ai.traceable.anomaly.config.service.v1.detector.ConfigValueType;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ApiProtectThreatRuleConfigMappingProvider {
  private static final String MAX_DEPTH = "max_depth";
  private static final String MIN_PERCENT_SEEN = "min_percent_seen";
  private static final String EVALUATE_REQUEST_BODY_PARAMS = "evaluate_request_body_params";
  private static final String ENABLED_FOR_INTERNAL_IPS = "enabled_for_internal_ips";
  private static final String THRESHOLDS_FAMILIES_EXCLUDED = "thresholds_families_excluded";
  private static final String SEVERE_REGEX_STRINGS = "severe_regex_strings";
  private static final String AUTH_REGEX_STRINGS = "auth_regex_strings";
  private static final String CSRF_REGEX_STRINGS = "csrf_regex_strings";
  private static final String NO_ANOMALY_STRINGS = "no_anomaly_strings";
  private static final String MODSECURITY_ENABLED = "modsecurity_enabled";
  private static final String MAX_LENGTH_DIFFERENCE = "max_length_difference";
  private static final String VALID_HTTP_STATUS_CODES = "valid_http_status_codes";
  private static final String VALID_GRPC_STATUS_CODES = "valid_grpc_status_codes";
  private static final String MAX_DIFFERENCE_RATIO = "max_difference_ratio";
  private static final String REQUEST_RANGE_SIZE = "request_range_size";
  private static final String RESPONSE_RANGE_SIZE = "response_range_size";
  private static final String ALLOWED_PROTOCOLS = "allowed_protocols";
  private static final String REQUIRED_OCCURRENCES_OF_HOST = "required_occurrences_of_host";
  private static final String ALLOWED_DOMAINS = "allowed_domains";
  private static final String DISABLED_FOR_UNKNOWN_ROLES = "disabled_for_unknown_roles";
  private static final String REQUIRE_USER_DEFINED_SCHEME = "require_user_defined_scheme";
  private static final String ROLE_REGEX = "role_regex";
  private static final String SCOPE_REGEX = "scope_regex";
  private static final String TIME_DIFFERENCE_BUFFER_MILLIS = "time_difference_buffer_millis";
  private static final String MIN_TOTAL_TRAFFIC_SEEN = "min_total_traffic_seen";
  private static final String EXCLUDE_SPECIAL_CHARACTERS = "exclude_special_characters";
  private static final String MAX_DEPTH_DIFFERENCE_ALLOWED = "max_depth_difference_allowed";
  private static final String URL_REGEX_TO_REQD_PARAM_REGEX_DETAILS =
      "url_regex_to_reqd_param_regex_details";
  private static final String MAX_ALIASES = "max_aliases";
  private static final String MAX_DUPLICATES = "max_duplicates";
  private static final String MAX_BATCHES = "max_batches";
  private static final String FIELD_DENY_LIST_REGEX = "field_deny_list_regex";
  private static final String OPERATION_DENY_LIST_REGEX = "operation_deny_list_regex";
  private static final String FIELD_DENY_LIST = "field_deny_list";
  private static final String OPERATION_DENY_LIST = "operation_deny_list";
  private static final String MIN_CORRELATION_PROBABILITY = "min_correlation_probability";
  private static final String PAIRWISE_CORRELATION_PROBABILITY = "pairwise_correlation_probability";
  private static final String ANY_SOURCE_CORRELATION_PROBABILITY =
      "any_source_correlation_probability";
  private static final String DISABLED_FOR_MISSING_PRECEDING_PARAM =
      "disabled_for_missing_preceding_param";
  private static final String ENABLED_ON_SAME_API = "enabled_on_same_api";
  private static final String PRECEDING_EVENT_LOOK_AHEAD_TIME_WINDOW =
      "preceding_event_look_ahead_time_window";
  private static final String REQUEST_PARAM_VALUES_NOT_ALLOWED = "request_param_values_not_allowed";
  private static final String MULTI_VALUED_STRING_PARAM_RULES = "multi_valued_string_param_rules";
  private static final String USER_ID_SOURCE = "user_id_source";
  private static final String USER_ID_DATA_LIST = "user_id_data_list";
  private static final String IP_TYPES = "ip_types";
  private static final String MIN_IP_ABUSE_VELOCITY = "min_ip_abuse_velocity";
  private static final String MIN_IP_REPUTATION_SCORE = "min_ip_reputation_score";
  private static final String UNIQUE_USER_CITIES_MIN_COUNT = "unique_user_cities_min_count";
  private static final String UNIQUE_EXTERNAL_IP_ADDRESSES_MIN_COUNT =
      "unique_external_ip_addresses_min_count";
  private static final String DISALLOW_UNAUTHENTICATED_SESSIONS =
      "disallow_unauthenticated_sessions";
  private static final String PRIMARY_API_MODEL_TYPE = "primary_api_model";
  private static final String SECONDARY_API_MODEL_TYPE = "secondary_api_model";
  private static final String USE_LEARNT_MODEL_FOR_PARAM_TYPE_INFO_MISSING =
      "use_learnt_model_for_param_type_info_missing";
  private static final String USE_LEARNT_MODEL_FOR_MISSING_RESPONSE_CODE =
      "use_learnt_model_for_missing_response_code";
  // rule-id
  public static final String JWT_THREAT_TYPE_ID = "jwt";
  public static final String GQLA_THREAT_TYPE_ID = "gqla";
  public static final String SESSIONV_THREAT_TYPE_ID = "sessionv";
  public static final String BOLA_THREAT_TYPE_ID = "bola";
  public static final String USER_ID_BOLA_THREAT_TYPE_ID = "userIdBola";
  public static final String BFLA_THREAT_TYPE_ID = "bfla";
  public static final String MISSING_PARAM_THREAT_TYPE_ID = "missingParam";
  public static final String SSRF_THREAT_TYPE_ID = "ssrf";
  public static final String CONTENT_SIZE_THREAT_TYPE_ID = "contentSize";
  public static final String CONTENT_TYPE_THREAT_TYPE_ID = "contentType";
  public static final String CONTENT_EXPLOSION_THREAT_TYPE_ID = "contentExplosion";
  public static final String SPECIAL_CHARACTER_THREAT_TYPE_ID = "specialCharacter";
  public static final String INTEGER_THREAT_TYPE_ID = "integer";
  public static final String DEVICE_THREAT_TYPE_ID = "device";
  public static final String ENUM_THREAT_TYPE_ID = "enum";
  public static final String UNKNOWN_PARAM_THREAT_TYPE_ID = "unknownParam";
  public static final String TYPE_THREAT_TYPE_ID = "type";
  public static final String HTTP_STATUS_THREAT_TYPE_ID = "httpStatus";
  public static final String CONTENT_ANOMALY_THREAT_TYPE_ID = "contentAnomaly";
  public static final String SCHEMA_VALIDATION_THREAT_TYPE_ID = "schemaValidation";
  public static final String AUTHZH_THREAT_TYPE_ID = "authzh";
  public static final String AUTHZV_THREAT_TYPE_ID = "authzv";
  public static final String CSTA_THREAT_TYPE_ID = "csta";
  public static final String AUTHN_THREAT_TYPE_ID = "authn";
  public static final String PARAMETER_ANOMALY_THREAT_TYPE_ID = "parameterAnomaly";
  public static final String JWT_EXP_SUB_RULE_ID = "jwt_exp";
  public static final String JWT_NBF_SUB_RULE_ID = "jwt_nbf";
  public static final String JWT_ISS_SUB_RULE_ID = "jwt_iss";
  public static final String JWT_AUD_SUB_RULE_ID = "jwt_aud";
  public static final String JWT_ALG_SUB_RULE_ID = "jwt_alg";
  public static final String JWT_SIGN_SUB_RULE_ID = "jwt_sign";
  public static final String JWT_ALBEAST_SUB_RULE_ID = "jwt_albeast";
  public static final String JWT_JKU_SUB_RULE_ID = "jwt_jku";
  public static final String GQLA_ABA_SUB_RULE_ID = "gqla_aba";
  public static final String GQLA_DRQ_SUB_RULE_ID = "gqla_drq";
  public static final String GQLA_FDA_SUB_RULE_ID = "gqla_fda";
  public static final String GQLA_BQA_SUB_RULE_ID = "gqla_bqa";
  public static final String GQLA_CF_SUB_RULE_ID = "gqla_cf";
  public static final String GQLA_IQ_SUB_RULE_ID = "gqla_iq";
  public static final String GQLA_RIQ_SUB_RULE_ID = "gqla_riq";
  public static final String GQLA_FS_SUB_RULE_ID = "gqla_fs";
  public static final String GQLA_GST_SUB_RULE_ID = "gqla_gst";
  public static final String GQLA_GDLB_SUB_RULE_ID = "gqla_gdlb";
  public static final String AUTHZH_BOLA_SUB_RULE_ID = "authzh_obola";
  public static final String AUTHZH_USER_ID_BOLA_SUB_RULE_ID = "authzh_ubola";
  public static final String AUTHZV_BFLA_SUB_RULE_ID = "authzv_bfla";
  public static final String CSTA_CSRF_SUB_RULE_ID = "csta_csrf";
  public static final String AUTHN_UA_SUB_RULE_ID = "authn_ua";
  public static final String SESSIONV_LANDSPEED_SUB_RULE_ID = "sessionv_landspeed";
  public static final String SESSIONV_EXPIRY_SUB_RULE_ID = "sessionv_expiry";
  public static final String SCHEMA_VALIDATION_MREQP_SUB_RULE_ID = "schemaValidation_mreqp";
  public static final String SCHEMA_VALIDATION_REQCTVE_SUB_RULE_ID = "schemaValidation_reqctve";
  public static final String SCHEMA_VALIDATION_RESCTVE_SUB_RULE_ID = "schemaValidation_resctve";
  public static final String SCHEMA_VALIDATION_REQIE_SUB_RULE_ID = "schemaValidation_reqie";
  public static final String SCHEMA_VALIDATION_UREQP_SUB_RULE_ID = "schemaValidation_ureqp";
  public static final String SCHEMA_VALIDATION_REQPTVE_SUB_RULE_ID = "schemaValidation_reqptve";
  public static final String SCHEMA_VALIDATION_URESC_SUB_RULE_ID = "schemaValidation_uresc";
  public static final String SSRF_UH_SUB_RULE_ID = "ssrf_uh";
  public static final String SSRF_UP_SUB_RULE_ID = "ssrf_up";
  public static final String SSRF_MH_SUB_RULE_ID = "ssrf_mh";
  public static final String CONTENT_ANOMALY_UREQCL_SUB_RULE_ID = "contentAnomaly_ureqcl";
  public static final String CONTENT_ANOMALY_URESCL_SUB_RULE_ID = "contentAnomaly_urescl";
  public static final String CONTENT_ANOMALY_REQCTM_SUB_RULE_ID = "contentAnomaly_reqctm";
  public static final String CONTENT_ANOMALY_REQCE_SUB_RULE_ID = "contentAnomaly_reqce";
  public static final String PARAMETER_ANOMALY_SCT_SUB_RULE_ID = "parameterAnomaly_sct";
  public static final String PARAMETER_ANOMALY_INTVOR_SUB_RULE_ID = "parameterAnomaly_intvor";
  public static final String PARAMETER_ANOMALY_INTUNS_SUB_RULE_ID = "parameterAnomaly_intuns";
  public static final String PARAMETER_ANOMALY_UUAD_SUB_RULE_ID = "parameterAnomaly_uuad";

  private static final Map<String, List<ConfigMetadata>> threatRuleIdToConfigMetadataMapping;

  static {
    threatRuleIdToConfigMetadataMapping = new HashMap<>();
    ConfigMetadata maxDepthConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MAX_DEPTH)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata minPercentSeenConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MIN_PERCENT_SEEN)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata urlRegexToReqdParamRegexDetailsConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(URL_REGEX_TO_REQD_PARAM_REGEX_DETAILS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_OBJECT)
                    .build())
            .build();

    ConfigMetadata evaluateRequestBodyParamsConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(EVALUATE_REQUEST_BODY_PARAMS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_BOOL)
                    .build())
            .build();

    ConfigMetadata enabledForInternalIpsConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(ENABLED_FOR_INTERNAL_IPS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_BOOL)
                    .build())
            .build();

    ConfigMetadata thresholdFamiliesExcluded =
        ConfigMetadata.newBuilder()
            .setKey(THRESHOLDS_FAMILIES_EXCLUDED)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata severeRegexStrings =
        ConfigMetadata.newBuilder()
            .setKey(SEVERE_REGEX_STRINGS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata authRegexStrings =
        ConfigMetadata.newBuilder()
            .setKey(AUTH_REGEX_STRINGS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata csrfRegexStrings =
        ConfigMetadata.newBuilder()
            .setKey(CSRF_REGEX_STRINGS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata noAnomalyStrings =
        ConfigMetadata.newBuilder()
            .setKey(NO_ANOMALY_STRINGS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata modsecurityEnabledConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MODSECURITY_ENABLED)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_BOOL)
                    .build())
            .build();

    ConfigMetadata maxLengthDifferenceConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MAX_LENGTH_DIFFERENCE)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata validHttpStatusCodesConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(VALID_HTTP_STATUS_CODES)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata validGrpcStatusCodesConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(VALID_GRPC_STATUS_CODES)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata maxDifferenceRatioConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MAX_DIFFERENCE_RATIO)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata requestRangeSizeConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(REQUEST_RANGE_SIZE)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata responseRangeSizeConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(RESPONSE_RANGE_SIZE)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata allowedProtocolsConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(ALLOWED_PROTOCOLS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata requiredOccurrencesOfHostConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(REQUIRED_OCCURRENCES_OF_HOST)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata allowedDomainsConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(ALLOWED_DOMAINS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata disabledForUnknownRolesConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(DISABLED_FOR_UNKNOWN_ROLES)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_BOOL)
                    .build())
            .build();

    ConfigMetadata requireUserDefinedSchemeConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(REQUIRE_USER_DEFINED_SCHEME)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_BOOL)
                    .build())
            .build();

    ConfigMetadata roleRegexConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(ROLE_REGEX)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_STRING)
                    .build())
            .build();

    ConfigMetadata scopeRegexConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(SCOPE_REGEX)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_STRING)
                    .build())
            .build();

    ConfigMetadata maxBatchesConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MAX_BATCHES)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata maxAliasesConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MAX_ALIASES)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata maxDuplicatesConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MAX_DUPLICATES)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata timeDifferenceBufferMillisConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(TIME_DIFFERENCE_BUFFER_MILLIS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata minTotalTrafficSeenConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MIN_TOTAL_TRAFFIC_SEEN)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata excludeSpecialCharactersConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(EXCLUDE_SPECIAL_CHARACTERS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_BOOL)
                    .build())
            .build();

    ConfigMetadata maxDepthDifferenceAllowedConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MAX_DEPTH_DIFFERENCE_ALLOWED)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata fieldDenyListRegexConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(FIELD_DENY_LIST_REGEX)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_STRING)
                    .build())
            .build();

    ConfigMetadata operationDenyListRegexConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(OPERATION_DENY_LIST_REGEX)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_STRING)
                    .build())
            .build();

    ConfigMetadata fieldDenyListConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(FIELD_DENY_LIST)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata operationDenyListConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(OPERATION_DENY_LIST)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata minCorrelationProbabilityConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MIN_CORRELATION_PROBABILITY)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata pairwiseCorrelationProbabilityConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(PAIRWISE_CORRELATION_PROBABILITY)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata anySourceCorrelationProbabilityConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(ANY_SOURCE_CORRELATION_PROBABILITY)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata disabledForMissingPrecedingParamConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(DISABLED_FOR_MISSING_PRECEDING_PARAM)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_BOOL)
                    .build())
            .build();

    ConfigMetadata enabledOnSameApiConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(ENABLED_ON_SAME_API)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_BOOL)
                    .build())
            .build();

    ConfigMetadata precedingEventLookAheadTimeWindowConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(PRECEDING_EVENT_LOOK_AHEAD_TIME_WINDOW)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata requestParamValuesNotAllowedConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(REQUEST_PARAM_VALUES_NOT_ALLOWED)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata multiValuedStringParamRulesConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MULTI_VALUED_STRING_PARAM_RULES)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_OBJECT)
                    .build())
            .build();

    ConfigMetadata userIdSourceConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(USER_ID_SOURCE)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_STRING)
                    .build())
            .build();

    ConfigMetadata userIdDataListConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(USER_ID_DATA_LIST)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata ipTypesConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(IP_TYPES)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata minIpAbuseVelocityConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MIN_IP_ABUSE_VELOCITY)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata minIpReputationScoreConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(MIN_IP_REPUTATION_SCORE)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata uniqueUserCitiesMinCountConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(UNIQUE_USER_CITIES_MIN_COUNT)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata uniqueExternalIpAddressesMinCountConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(UNIQUE_EXTERNAL_IP_ADDRESSES_MIN_COUNT)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_NUMBER)
                    .build())
            .build();

    ConfigMetadata disallowUnauthenticatedSessionsConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(DISALLOW_UNAUTHENTICATED_SESSIONS)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_BOOL)
                    .build())
            .build();
    ConfigMetadata primaryApiModelTypeConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(PRIMARY_API_MODEL_TYPE)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_STRING)
                    .build())
            .build();

    ConfigMetadata secondaryApiModelTypeConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(SECONDARY_API_MODEL_TYPE)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_STRING)
                    .build())
            .build();
    ConfigMetadata useLearntModelForParamTypeInfoMissingConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(USE_LEARNT_MODEL_FOR_PARAM_TYPE_INFO_MISSING)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_BOOL)
                    .build())
            .build();
    ConfigMetadata useLearntModelForMissingResponseCodeConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(USE_LEARNT_MODEL_FOR_MISSING_RESPONSE_CODE)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_BOOL)
                    .build())
            .build();

    // jwt_exp mapping
    threatRuleIdToConfigMetadataMapping.put(
        JWT_EXP_SUB_RULE_ID, List.of(timeDifferenceBufferMillisConfigMetadata));

    // jwt_nbf mapping
    threatRuleIdToConfigMetadataMapping.put(
        JWT_NBF_SUB_RULE_ID, List.of(timeDifferenceBufferMillisConfigMetadata));

    // jwt_iss mapping
    threatRuleIdToConfigMetadataMapping.put(
        JWT_ISS_SUB_RULE_ID, List.of(minPercentSeenConfigMetadata));

    // jwt_aud mapping
    threatRuleIdToConfigMetadataMapping.put(
        JWT_AUD_SUB_RULE_ID, List.of(minPercentSeenConfigMetadata));

    // jwt_alg mapping
    threatRuleIdToConfigMetadataMapping.put(
        JWT_ALG_SUB_RULE_ID, List.of(minPercentSeenConfigMetadata));

    // jwt_sign mapping
    threatRuleIdToConfigMetadataMapping.put(
        JWT_SIGN_SUB_RULE_ID, List.of(minPercentSeenConfigMetadata));

    // jwt_albeast mapping
    threatRuleIdToConfigMetadataMapping.put(
        JWT_ALBEAST_SUB_RULE_ID, List.of(minPercentSeenConfigMetadata));

    // jwt_jku mapping
    threatRuleIdToConfigMetadataMapping.put(
        JWT_JKU_SUB_RULE_ID, List.of(minPercentSeenConfigMetadata));

    // gqla_aba mapping
    threatRuleIdToConfigMetadataMapping.put(
        GQLA_ABA_SUB_RULE_ID, List.of(maxAliasesConfigMetadata));

    // gqla_drq mapping
    threatRuleIdToConfigMetadataMapping.put(GQLA_DRQ_SUB_RULE_ID, List.of(maxDepthConfigMetadata));

    // gqla_fda mapping
    threatRuleIdToConfigMetadataMapping.put(
        GQLA_FDA_SUB_RULE_ID, List.of(maxDuplicatesConfigMetadata));

    // gqla_bqa mapping
    threatRuleIdToConfigMetadataMapping.put(
        GQLA_BQA_SUB_RULE_ID, List.of(maxBatchesConfigMetadata));

    // gqla_cf mapping
    threatRuleIdToConfigMetadataMapping.put(GQLA_CF_SUB_RULE_ID, List.of());

    // gqla_iq mapping
    threatRuleIdToConfigMetadataMapping.put(GQLA_IQ_SUB_RULE_ID, List.of());

    // gqla_riq mapping
    threatRuleIdToConfigMetadataMapping.put(GQLA_RIQ_SUB_RULE_ID, List.of());

    // gqla_fs mapping
    threatRuleIdToConfigMetadataMapping.put(GQLA_FS_SUB_RULE_ID, List.of());

    // gqla_gst mapping
    threatRuleIdToConfigMetadataMapping.put(GQLA_GST_SUB_RULE_ID, List.of());

    // gqla_gdlb mapping
    threatRuleIdToConfigMetadataMapping.put(
        GQLA_GDLB_SUB_RULE_ID,
        List.of(
            fieldDenyListRegexConfigMetadata,
            fieldDenyListConfigMetadata,
            operationDenyListConfigMetadata,
            operationDenyListRegexConfigMetadata));

    // authzh_ubola mapping
    threatRuleIdToConfigMetadataMapping.put(
        AUTHZH_USER_ID_BOLA_SUB_RULE_ID,
        List.of(
            minCorrelationProbabilityConfigMetadata,
            userIdSourceConfigMetadata,
            userIdDataListConfigMetadata));

    // authzh_obola mapping
    threatRuleIdToConfigMetadataMapping.put(
        AUTHZH_BOLA_SUB_RULE_ID,
        List.of(
            minCorrelationProbabilityConfigMetadata,
            pairwiseCorrelationProbabilityConfigMetadata,
            anySourceCorrelationProbabilityConfigMetadata,
            disabledForMissingPrecedingParamConfigMetadata,
            enabledOnSameApiConfigMetadata,
            precedingEventLookAheadTimeWindowConfigMetadata,
            requestParamValuesNotAllowedConfigMetadata,
            multiValuedStringParamRulesConfigMetadata));

    // authzv_bfla mapping
    threatRuleIdToConfigMetadataMapping.put(
        AUTHZV_BFLA_SUB_RULE_ID,
        List.of(
            minPercentSeenConfigMetadata,
            disabledForUnknownRolesConfigMetadata,
            requireUserDefinedSchemeConfigMetadata,
            roleRegexConfigMetadata,
            scopeRegexConfigMetadata));

    // csta_csrf mapping
    threatRuleIdToConfigMetadataMapping.put(
        CSTA_CSRF_SUB_RULE_ID,
        List.of(
            minPercentSeenConfigMetadata,
            csrfRegexStrings,
            enabledForInternalIpsConfigMetadata,
            evaluateRequestBodyParamsConfigMetadata,
            thresholdFamiliesExcluded,
            urlRegexToReqdParamRegexDetailsConfigMetadata));

    // authn_ua mapping
    threatRuleIdToConfigMetadataMapping.put(
        AUTHN_UA_SUB_RULE_ID,
        List.of(
            minPercentSeenConfigMetadata,
            authRegexStrings,
            enabledForInternalIpsConfigMetadata,
            evaluateRequestBodyParamsConfigMetadata,
            thresholdFamiliesExcluded));

    // sessionv_expiry mapping
    threatRuleIdToConfigMetadataMapping.put(
        SESSIONV_EXPIRY_SUB_RULE_ID,
        List.of(
            timeDifferenceBufferMillisConfigMetadata,
            disallowUnauthenticatedSessionsConfigMetadata));

    // sessionv_landspeed mapping
    threatRuleIdToConfigMetadataMapping.put(
        SESSIONV_LANDSPEED_SUB_RULE_ID,
        List.of(
            disallowUnauthenticatedSessionsConfigMetadata,
            minIpReputationScoreConfigMetadata,
            uniqueExternalIpAddressesMinCountConfigMetadata,
            uniqueUserCitiesMinCountConfigMetadata,
            minIpAbuseVelocityConfigMetadata,
            ipTypesConfigMetadata));

    // ssrf_uh mapping
    threatRuleIdToConfigMetadataMapping.put(
        SSRF_UH_SUB_RULE_ID,
        List.of(allowedDomainsConfigMetadata, requiredOccurrencesOfHostConfigMetadata));

    // ssrf_up mapping
    threatRuleIdToConfigMetadataMapping.put(
        SSRF_UP_SUB_RULE_ID, List.of(allowedProtocolsConfigMetadata));

    // ssrf_mh mapping
    threatRuleIdToConfigMetadataMapping.put(
        SSRF_MH_SUB_RULE_ID, List.of(allowedDomainsConfigMetadata));

    // contentAnomaly_ureqcl mapping
    threatRuleIdToConfigMetadataMapping.put(
        CONTENT_ANOMALY_UREQCL_SUB_RULE_ID,
        List.of(
            minPercentSeenConfigMetadata,
            maxDifferenceRatioConfigMetadata,
            requestRangeSizeConfigMetadata,
            enabledForInternalIpsConfigMetadata));

    // contentAnomaly_urescl mapping
    threatRuleIdToConfigMetadataMapping.put(
        CONTENT_ANOMALY_URESCL_SUB_RULE_ID,
        List.of(
            minPercentSeenConfigMetadata,
            maxDifferenceRatioConfigMetadata,
            responseRangeSizeConfigMetadata,
            enabledForInternalIpsConfigMetadata));

    // contentAnomaly_reqctm mapping
    threatRuleIdToConfigMetadataMapping.put(
        CONTENT_ANOMALY_REQCTM_SUB_RULE_ID,
        List.of(modsecurityEnabledConfigMetadata, enabledForInternalIpsConfigMetadata));

    // contentAnomaly_reqce mapping
    threatRuleIdToConfigMetadataMapping.put(
        CONTENT_ANOMALY_REQCE_SUB_RULE_ID,
        List.of(minPercentSeenConfigMetadata, maxDepthDifferenceAllowedConfigMetadata));

    // parameterAnomaly_sct mapping
    threatRuleIdToConfigMetadataMapping.put(
        PARAMETER_ANOMALY_SCT_SUB_RULE_ID,
        List.of(
            excludeSpecialCharactersConfigMetadata,
            minPercentSeenConfigMetadata,
            minTotalTrafficSeenConfigMetadata));

    // parameterAnomaly_intvor mapping
    threatRuleIdToConfigMetadataMapping.put(
        PARAMETER_ANOMALY_INTVOR_SUB_RULE_ID, List.of(maxLengthDifferenceConfigMetadata));

    // parameterAnomaly_intuns mapping
    threatRuleIdToConfigMetadataMapping.put(PARAMETER_ANOMALY_INTUNS_SUB_RULE_ID, List.of());

    // parameterAnomaly_uuad mapping
    threatRuleIdToConfigMetadataMapping.put(
        PARAMETER_ANOMALY_UUAD_SUB_RULE_ID, List.of(minPercentSeenConfigMetadata));

    // schemaValidation_reqctve mapping
    threatRuleIdToConfigMetadataMapping.put(
        SCHEMA_VALIDATION_REQCTVE_SUB_RULE_ID,
        List.of(
            minPercentSeenConfigMetadata,
            modsecurityEnabledConfigMetadata,
            enabledForInternalIpsConfigMetadata,
            primaryApiModelTypeConfigMetadata,
            secondaryApiModelTypeConfigMetadata));

    // schemaValidation_resctve mapping
    threatRuleIdToConfigMetadataMapping.put(
        SCHEMA_VALIDATION_RESCTVE_SUB_RULE_ID,
        List.of(
            minPercentSeenConfigMetadata,
            modsecurityEnabledConfigMetadata,
            enabledForInternalIpsConfigMetadata,
            primaryApiModelTypeConfigMetadata,
            secondaryApiModelTypeConfigMetadata));

    // schemaValidation_reqie mapping
    threatRuleIdToConfigMetadataMapping.put(
        SCHEMA_VALIDATION_REQIE_SUB_RULE_ID,
        List.of(
            noAnomalyStrings,
            modsecurityEnabledConfigMetadata,
            enabledForInternalIpsConfigMetadata,
            thresholdFamiliesExcluded,
            primaryApiModelTypeConfigMetadata,
            secondaryApiModelTypeConfigMetadata));

    // schemaValidation_reqptve mapping
    threatRuleIdToConfigMetadataMapping.put(
        SCHEMA_VALIDATION_REQPTVE_SUB_RULE_ID,
        List.of(
            noAnomalyStrings,
            modsecurityEnabledConfigMetadata,
            enabledForInternalIpsConfigMetadata,
            primaryApiModelTypeConfigMetadata,
            secondaryApiModelTypeConfigMetadata,
            useLearntModelForParamTypeInfoMissingConfigMetadata));

    // schemaValidation_uresc mapping
    threatRuleIdToConfigMetadataMapping.put(
        SCHEMA_VALIDATION_URESC_SUB_RULE_ID,
        List.of(
            validGrpcStatusCodesConfigMetadata,
            validHttpStatusCodesConfigMetadata,
            minPercentSeenConfigMetadata,
            primaryApiModelTypeConfigMetadata,
            secondaryApiModelTypeConfigMetadata,
            useLearntModelForMissingResponseCodeConfigMetadata));

    // schemaValidation_ureqp mapping
    threatRuleIdToConfigMetadataMapping.put(
        SCHEMA_VALIDATION_UREQP_SUB_RULE_ID,
        List.of(
            modsecurityEnabledConfigMetadata,
            enabledForInternalIpsConfigMetadata,
            thresholdFamiliesExcluded,
            severeRegexStrings,
            primaryApiModelTypeConfigMetadata,
            secondaryApiModelTypeConfigMetadata));

    // schemaValidation_mreqp mapping
    threatRuleIdToConfigMetadataMapping.put(
        SCHEMA_VALIDATION_MREQP_SUB_RULE_ID,
        List.of(
            minPercentSeenConfigMetadata,
            severeRegexStrings,
            enabledForInternalIpsConfigMetadata,
            evaluateRequestBodyParamsConfigMetadata,
            thresholdFamiliesExcluded,
            primaryApiModelTypeConfigMetadata,
            secondaryApiModelTypeConfigMetadata));
  }

  public static List<ConfigMetadata> getConfigMetadata(String threatRuleId) {
    return threatRuleIdToConfigMetadataMapping.get(threatRuleId);
  }
}
