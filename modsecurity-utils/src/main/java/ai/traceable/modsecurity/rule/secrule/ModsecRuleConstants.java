package ai.traceable.modsecurity.rule.secrule;

import com.google.re2j.Pattern;

public abstract class ModsecRuleConstants {
  public static final String SEC_RULE = "SecRule";
  public static final String SPACE_DELIMITER = " ";
  public static final String NEW_LINE_DELIMITER = "\n";
  public static final String PIPE = "|";
  public static final String AT_PREFIX = "@";
  public static final String NOT_PREFIX = "!";
  public static final String DOUBLE_QUOTES = "\"";
  public static final String COLON = ":";
  public static final String FRONT_SLASH = "/";
  public static final Pattern SEC_RULE_DIRECTIVES_WITH_CHAIN_KEYWORDS_REGEX =
      Pattern.compile(
          "(SecAction|SecRule|SecRuleScript)(\\s+chain\\s+(SecAction|SecRule|SecRuleScript))*");
  public static final java.util.regex.Pattern SEC_RULE_ID_REGEX =
      java.util.regex.Pattern.compile(
          "id[ \\t\\x0B\\f\\r]{0,10}:[ \\t\\x0B\\f\\r]{0,10}\\d+[ \\t\\x0B\\f\\r]{0,10},");
}
