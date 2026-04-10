package ai.traceable.edge.decision.converter.utils;

public class Constants {
  private Constants() {
    // utility classes shouldn't have a public constructor
  }

  public static final String IP_ABUSE_VELOCITY_COMPARISON_JEXL_EXP =
      "$s.getIpIntelligenceData().getTraits().getAbuseVelocity().equals(IpAbuseVelocity.%s)";
  public static final String IP_ADDRESS_JEXL_EXP = "$s.getIpAddress()";
  public static final String IS_IP_IN_RANGE_JEXL_EXP =
      "ipValidation:isIpAddressInRange('%s', $s.getIpAddress())";
  public static final String EXTERNAL_IP_JEXL_EXP = "$s.getIpValidationResult().isExternalIp()";
  public static final String INTERNAL_IP_JEXL_EXP =
      JexlUtils.getNotJexlExpression(EXTERNAL_IP_JEXL_EXP);
  public static final String IP_ASN_JEXL_EXP = "$s.getIpIntelligenceData().getNetwork().getAsn()";
  public static final String IP_CONNECTION_TYPE_COMPARISON_JEXL_EXP =
      "$s.getIpIntelligenceData().getTraits().getConnectionType().equals(IpConnectionType.%s)";
  public static final String IP_ORGANISATION_JEXL_EXP =
      "$s.getIpIntelligenceData().getIspData().getOrganization()";
  public static final String IP_REPUTATION_LEVEL_JEXL_EXP =
      "$s.getIpIntelligenceData().getIpReputationLevel()";
  public static final String IP_TYPE_COMPARISON_JEXL_EXP =
      "$s.getIpIntelligenceData().getTraits().getIpTypes().contains(IPType.%s)";
  public static final String COUNTRY_ISO_CODE_JEXL_EXP =
      "$s.getIpIntelligenceData().getCountry().getIsoCode()";
  public static final String STATE_NAME_JEXL_EXP =
      "$s.getIpIntelligenceData().getState().getName()";
  public static final String CITY_NAME_JEXL_EXP = "$s.getIpIntelligenceData().getCity().getName()";
  public static final String USER_AGENT_JEXL_EXP = "$s.getUserAgent()";
  public static final String ATTRIBUTE_NAME_LHS = "lhs";
  public static final String PATH_JEXL_EXP = "$s.getPath()";
  public static final String SERVICE_JEXL_EXP = "$s.getServiceName()";

  public static final String VAR_REGION_COUNTRY_ISO = "region.country_iso";
  public static final String VAR_REGION_STATE_NAME = "region.state_name";
  public static final String VAR_REGION_CITY_NAME = "region.city_name";
}
