package org.hypertrace.config.validation;

import com.google.re2j.Pattern;
import com.google.re2j.PatternSyntaxException;
import io.grpc.Status;

public class RegexValidator {
  public static Status validate(String regexPattern) {
    // compiling an invalid regex throws PatternSyntaxException
    try {
      Pattern.compile(regexPattern);
    } catch (PatternSyntaxException ex) {
      return Status.INVALID_ARGUMENT
          .withCause(ex)
          .withDescription("Invalid Regex pattern: " + regexPattern);
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
}
