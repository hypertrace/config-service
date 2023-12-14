package org.hypertrace.label.application.rule.config.service;

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
}
