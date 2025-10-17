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
  private static final String TIME_DIFFERENCE_BUFFER_MILLIS = "time_difference_buffer_millis";
  private static final String MIN_TOTAL_TRAFFIC_SEEN = "min_total_traffic_seen";
  private static final String EXCLUDE_SPECIAL_CHARACTERS = "exclude_special_characters";
  private static final String MAX_DEPTH_DIFFERENCE_ALLOWED = "max_depth_difference_allowed";
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
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
                    .build())
            .build();

    ConfigMetadata operationDenyListRegexConfigMetadata =
        ConfigMetadata.newBuilder()
            .setKey(OPERATION_DENY_LIST_REGEX)
            .setConfigValueMetadata(
                ConfigValueMetadata.newBuilder()
                    .setType(ConfigValueType.CONFIG_VALUE_TYPE_ARRAY)
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

    // jwt_exp mapping
    threatRuleIdToConfigMetadataMapping.put(
        "jwt_exp", List.of(timeDifferenceBufferMillisConfigMetadata));

    // jwt_nbf mapping
    threatRuleIdToConfigMetadataMapping.put(
        "jwt_nbf", List.of(timeDifferenceBufferMillisConfigMetadata));

    // jwt_iss mapping
    threatRuleIdToConfigMetadataMapping.put("jwt_iss", List.of(minPercentSeenConfigMetadata));

    // jwt_aud mapping
    threatRuleIdToConfigMetadataMapping.put("jwt_aud", List.of(minPercentSeenConfigMetadata));

    // jwt_alg mapping
    threatRuleIdToConfigMetadataMapping.put("jwt_alg", List.of(minPercentSeenConfigMetadata));

    // jwt_sign mapping
    threatRuleIdToConfigMetadataMapping.put("jwt_sign", List.of(minPercentSeenConfigMetadata));

    // jwt_albeast mapping
    threatRuleIdToConfigMetadataMapping.put("jwt_albeast", List.of(minPercentSeenConfigMetadata));

    // jwt_jku mapping
    threatRuleIdToConfigMetadataMapping.put("jwt_jku", List.of(minPercentSeenConfigMetadata));

    // gqla_aba mapping
    threatRuleIdToConfigMetadataMapping.put("gqla_aba", List.of(maxAliasesConfigMetadata));

    // gqla_drq mapping
    threatRuleIdToConfigMetadataMapping.put("gqla_drq", List.of(maxDepthConfigMetadata));

    // gqla_fda mapping
    threatRuleIdToConfigMetadataMapping.put("gqla_fda", List.of(maxDuplicatesConfigMetadata));

    // gqla_bqa mapping
    threatRuleIdToConfigMetadataMapping.put("gqla_bqa", List.of(maxBatchesConfigMetadata));

    // gqla_cf mapping
    threatRuleIdToConfigMetadataMapping.put("gqla_cf", List.of());

    // gqla_iq mapping
    threatRuleIdToConfigMetadataMapping.put("gqla_iq", List.of());

    // gqla_riq mapping
    threatRuleIdToConfigMetadataMapping.put("gqla_riq", List.of());

    // gqla_fs mapping
    threatRuleIdToConfigMetadataMapping.put("gqla_fs", List.of());

    // gqla_gst mapping
    threatRuleIdToConfigMetadataMapping.put("gqla_gst", List.of());

    // gqla_gdlb mapping
    threatRuleIdToConfigMetadataMapping.put(
        "gqla_gdlb",
        List.of(
            fieldDenyListRegexConfigMetadata,
            fieldDenyListConfigMetadata,
            operationDenyListConfigMetadata,
            operationDenyListRegexConfigMetadata));

    // authz_bfla mapping
    threatRuleIdToConfigMetadataMapping.put(
        "authz_bfla", List.of(minPercentSeenConfigMetadata, disabledForUnknownRolesConfigMetadata));

    // authz_ubola mapping
    threatRuleIdToConfigMetadataMapping.put(
        "authz_ubola",
        List.of(
            minCorrelationProbabilityConfigMetadata,
            userIdSourceConfigMetadata,
            userIdDataListConfigMetadata));

    // authz_obola mapping
    threatRuleIdToConfigMetadataMapping.put(
        "authz_obola",
        List.of(
            minCorrelationProbabilityConfigMetadata,
            pairwiseCorrelationProbabilityConfigMetadata,
            anySourceCorrelationProbabilityConfigMetadata,
            disabledForMissingPrecedingParamConfigMetadata,
            enabledOnSameApiConfigMetadata,
            precedingEventLookAheadTimeWindowConfigMetadata,
            requestParamValuesNotAllowedConfigMetadata,
            multiValuedStringParamRulesConfigMetadata));

    // authz_csrf mapping
    threatRuleIdToConfigMetadataMapping.put(
        "authz_csrf",
        List.of(
            minPercentSeenConfigMetadata,
            csrfRegexStrings,
            enabledForInternalIpsConfigMetadata,
            evaluateRequestBodyParamsConfigMetadata,
            thresholdFamiliesExcluded));

    // authn_aua mapping
    threatRuleIdToConfigMetadataMapping.put(
        "authn_aua",
        List.of(
            minPercentSeenConfigMetadata,
            authRegexStrings,
            enabledForInternalIpsConfigMetadata,
            evaluateRequestBodyParamsConfigMetadata,
            thresholdFamiliesExcluded));

    // authn_ua mapping
    threatRuleIdToConfigMetadataMapping.put(
        "authn_ua",
        List.of(
            minPercentSeenConfigMetadata,
            authRegexStrings,
            enabledForInternalIpsConfigMetadata,
            evaluateRequestBodyParamsConfigMetadata,
            thresholdFamiliesExcluded));

    // sessionv_expiry mapping
    threatRuleIdToConfigMetadataMapping.put(
        "sessionv_expiry",
        List.of(
            timeDifferenceBufferMillisConfigMetadata,
            disallowUnauthenticatedSessionsConfigMetadata));

    // sessionv_landspeed mapping
    threatRuleIdToConfigMetadataMapping.put(
        "sessionv_landspeed",
        List.of(
            disallowUnauthenticatedSessionsConfigMetadata,
            minIpReputationScoreConfigMetadata,
            uniqueExternalIpAddressesMinCountConfigMetadata,
            uniqueUserCitiesMinCountConfigMetadata,
            minIpAbuseVelocityConfigMetadata,
            ipTypesConfigMetadata));

    // ssrf_uh mapping
    threatRuleIdToConfigMetadataMapping.put(
        "ssrf_uh", List.of(allowedDomainsConfigMetadata, requiredOccurrencesOfHostConfigMetadata));

    // ssrf_up mapping
    threatRuleIdToConfigMetadataMapping.put("ssrf_up", List.of(allowedProtocolsConfigMetadata));

    // ssrf_mh mapping
    threatRuleIdToConfigMetadataMapping.put("ssrf_mh", List.of(allowedDomainsConfigMetadata));

    // contentAnomaly_ureqcl mapping
    threatRuleIdToConfigMetadataMapping.put(
        "contentAnomaly_ureqcl",
        List.of(
            minPercentSeenConfigMetadata,
            maxDifferenceRatioConfigMetadata,
            requestRangeSizeConfigMetadata,
            enabledForInternalIpsConfigMetadata));

    // contentAnomaly_urescl mapping
    threatRuleIdToConfigMetadataMapping.put(
        "contentAnomaly_urescl",
        List.of(
            minPercentSeenConfigMetadata,
            maxDifferenceRatioConfigMetadata,
            responseRangeSizeConfigMetadata,
            enabledForInternalIpsConfigMetadata));

    // contentAnomaly_reqctm mapping
    threatRuleIdToConfigMetadataMapping.put(
        "contentAnomaly_reqctm",
        List.of(modsecurityEnabledConfigMetadata, enabledForInternalIpsConfigMetadata));

    // contentAnomaly_reqce mapping
    threatRuleIdToConfigMetadataMapping.put(
        "contentAnomaly_reqce",
        List.of(minPercentSeenConfigMetadata, maxDepthDifferenceAllowedConfigMetadata));

    // parameterAnomaly_sct mapping
    threatRuleIdToConfigMetadataMapping.put(
        "parameterAnomaly_sct",
        List.of(
            excludeSpecialCharactersConfigMetadata,
            minPercentSeenConfigMetadata,
            minTotalTrafficSeenConfigMetadata));

    // parameterAnomaly_intvor mapping
    threatRuleIdToConfigMetadataMapping.put(
        "parameterAnomaly_intvor", List.of(maxLengthDifferenceConfigMetadata));

    // parameterAnomaly_intuns mapping
    threatRuleIdToConfigMetadataMapping.put("parameterAnomaly_intuns", List.of());

    // parameterAnomaly_uuad mapping
    threatRuleIdToConfigMetadataMapping.put(
        "parameterAnomaly_uuad", List.of(minPercentSeenConfigMetadata));

    // schemaValidation_reqctve mapping
    threatRuleIdToConfigMetadataMapping.put(
        "schemaValidation_reqctve",
        List.of(
            minPercentSeenConfigMetadata,
            modsecurityEnabledConfigMetadata,
            enabledForInternalIpsConfigMetadata));

    // schemaValidation_resctve mapping
    threatRuleIdToConfigMetadataMapping.put(
        "schemaValidation_resctve",
        List.of(
            minPercentSeenConfigMetadata,
            modsecurityEnabledConfigMetadata,
            enabledForInternalIpsConfigMetadata));

    // schemaValidation_reqie mapping
    threatRuleIdToConfigMetadataMapping.put(
        "schemaValidation_reqie",
        List.of(
            noAnomalyStrings,
            modsecurityEnabledConfigMetadata,
            enabledForInternalIpsConfigMetadata,
            thresholdFamiliesExcluded));

    // schemaValidation_reqptve mapping
    threatRuleIdToConfigMetadataMapping.put(
        "schemaValidation_reqptve",
        List.of(
            noAnomalyStrings,
            modsecurityEnabledConfigMetadata,
            enabledForInternalIpsConfigMetadata));

    // schemaValidation_uresc mapping
    threatRuleIdToConfigMetadataMapping.put(
        "schemaValidation_uresc",
        List.of(
            validGrpcStatusCodesConfigMetadata,
            validHttpStatusCodesConfigMetadata,
            minPercentSeenConfigMetadata));

    // schemaValidation_ureqp mapping
    threatRuleIdToConfigMetadataMapping.put(
        "schemaValidation_ureqp",
        List.of(
            modsecurityEnabledConfigMetadata,
            enabledForInternalIpsConfigMetadata,
            thresholdFamiliesExcluded,
            severeRegexStrings));

    // schemaValidation_mreqp mapping
    threatRuleIdToConfigMetadataMapping.put(
        "schemaValidation_mreqp",
        List.of(
            minPercentSeenConfigMetadata,
            severeRegexStrings,
            enabledForInternalIpsConfigMetadata,
            evaluateRequestBodyParamsConfigMetadata,
            thresholdFamiliesExcluded));
  }

  public static List<ConfigMetadata> getConfigMetadata(String threatRuleId) {
    return threatRuleIdToConfigMetadataMapping.get(threatRuleId);
  }
}
