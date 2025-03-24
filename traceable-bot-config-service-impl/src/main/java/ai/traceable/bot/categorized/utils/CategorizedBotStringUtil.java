package ai.traceable.bot.categorized.utils;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotDetails;
import lombok.experimental.UtilityClass;

@UtilityClass
public class CategorizedBotStringUtil {

  public static final String TC_BOTS_PREFIX = "TRACEABLEAI_BOT";

  public String getFormattedBotName(final CategorizedBotDetails categorizedBotDetails) {
    return categorizedBotDetails.getName().toLowerCase().replaceAll("[^a-z]+", "_");
  }

  public String getFormattedBotVariableName(final String botName, final String botId) {
    return joinStrings(TC_BOTS_PREFIX, joinStrings(botName, botId));
  }

  public static String joinStrings(final String first, final String second) {
    return String.format("%s_%s", first, second);
  }
}
