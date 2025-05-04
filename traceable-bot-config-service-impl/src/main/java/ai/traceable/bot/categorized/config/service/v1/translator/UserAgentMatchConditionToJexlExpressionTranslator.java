package ai.traceable.bot.categorized.config.service.v1.translator;

import ai.traceable.bot.categorized.config.service.v1.UserAgentCondition;
import com.google.protobuf.ProtocolStringList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;

@UtilityClass
public class UserAgentMatchConditionToJexlExpressionTranslator {

  public static String buildJexlExpression(final UserAgentCondition userAgentCondition) {

    final String containsExpression =
        buildUserAgentContainsJexlString(userAgentCondition.getUserAgentsList());
    final String userAgentRegexExpression =
        buildUserAgentRegexMatchJexlString(userAgentCondition.getUserAgentRegexesList());
    if (!containsExpression.isEmpty() && !userAgentRegexExpression.isEmpty()) {
      return String.format("%s || %s", containsExpression, userAgentRegexExpression);
    } else if (!containsExpression.isEmpty()) {
      return containsExpression;
    } else {
      return userAgentRegexExpression;
    }
  }

  private static String buildUserAgentRegexMatchJexlString(
      final ProtocolStringList userAgentRegexesList) {
    return userAgentRegexesList.isEmpty()
        ? ""
        : "$s.getLowerCaseUserAgent() =~ " + joinRegexes(userAgentRegexesList);
  }

  private static String buildUserAgentContainsJexlString(final ProtocolStringList userAgentsList) {
    return userAgentsList.stream()
        .map(
            userAgent -> {
              String userAgentLower = userAgent.toLowerCase();
              return String.format("$s.getLowerCaseUserAgent().contains('%s')", userAgentLower);
            })
        .collect(Collectors.joining(" || "));
  }

  public static String joinRegexes(final List<String> regexList) {
    return String.join("|", regexList);
  }
}
