package ai.traceable.config.utils;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import dk.brics.automaton.Automaton;
import dk.brics.automaton.RegExp;
import io.grpc.Status;
import java.util.List;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RegexValidator {

  private static final List<Automaton> DEFAULT_WIDE_REGEX_AUTOMATON_LIST =
      List.of(new RegExp(".*").toAutomaton(), new RegExp("\\.*").toAutomaton());

  private RegexValidator() {}

  public static Status validate(String regexPattern) {
    // compiling an invalid regex throws PatternSyntaxException
    try {
      Pattern.compile(regexPattern);
    } catch (PatternSyntaxException e) {
      return Status.INVALID_ARGUMENT
          .withCause(e)
          .withDescription("Invalid Regex pattern: " + regexPattern);
    }
    if (isWideRegex(regexPattern)) {
      throw Status.INVALID_ARGUMENT
          .withDescription(String.format("Wide regex is not allowed : {}", regexPattern))
          .asRuntimeException();
    }
    return Status.OK;
  }

  public static Status validateCaptureGroupCount(String regexPattern, int expectedCount) {
    // compiling an invalid regex throws PatternSyntaxException
    try {
      Pattern pattern = Pattern.compile(regexPattern);
      if (pattern.groupCount() != expectedCount) {
        return Status.INVALID_ARGUMENT.withDescription(
            "Regex group count should be: " + expectedCount);
      }
    } catch (PatternSyntaxException e) {
      return Status.INVALID_ARGUMENT
          .withCause(e)
          .withDescription("Invalid Regex pattern: " + regexPattern);
    }
    return Status.OK;
  }

  public static void validateRegexes(List<String> regexes) {
    for (String regex : regexes) {
      if (isWideRegex(regex)) {
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Wide regex is not allowed : {}", regex))
            .asRuntimeException();
      }
      if (validate(regex) != Status.OK) {
        throw Status.INVALID_ARGUMENT
            .withDescription(String.format("Invalid regex is not allowed : {}", regex))
            .asRuntimeException();
      }
    }
  }

  private static boolean isWideRegex(String regex) {
    try {
      Automaton regAutomaton = new RegExp(regex).toAutomaton();
      if (DEFAULT_WIDE_REGEX_AUTOMATON_LIST.stream()
          .anyMatch(automaton -> automaton.equals(regAutomaton))) {
        return true;
      }
    } catch (Exception e) {
      return true;
    }
    return false;
  }
}
