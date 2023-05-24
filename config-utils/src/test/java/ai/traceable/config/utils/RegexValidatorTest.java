package ai.traceable.config.utils;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class RegexValidatorTest {

  @Test
  void testRegexCaptureValidation() {
    // expect 1
    assertFalse(RegexValidator.validateCaptureGroupCount("ex-(.*)", 0).isOk());
    assertTrue(RegexValidator.validateCaptureGroupCount("ex-(.*)", 1).isOk());
    assertFalse(RegexValidator.validateCaptureGroupCount("ex-(.*)", 2).isOk());
    // expect 2
    assertFalse(RegexValidator.validateCaptureGroupCount("ex-(.*)-(.*)", 0).isOk());
    assertFalse(RegexValidator.validateCaptureGroupCount("ex-(.*)-(.*)", 1).isOk());
    assertTrue(RegexValidator.validateCaptureGroupCount("ex-(.*)-(.*)", 2).isOk());
    // expect 0
    assertTrue(RegexValidator.validateCaptureGroupCount("ex-", 0).isOk());
    assertFalse(RegexValidator.validateCaptureGroupCount("ex-", 1).isOk());
    assertFalse(RegexValidator.validateCaptureGroupCount("ex-", 2).isOk());

    // malformed should fail even if otherwise matches
    assertFalse(RegexValidator.validateCaptureGroupCount("ex-(.*)-[", 1).isOk());

    // non-capturing match shouldn't count
    assertTrue(RegexValidator.validateCaptureGroupCount("ex-(?:.*)", 0).isOk());
    assertFalse(RegexValidator.validateCaptureGroupCount("ex-(?:.*)", 1).isOk());

    // named capture should count
    assertFalse(RegexValidator.validateCaptureGroupCount("(?P<test>.*)", 0).isOk());
    assertTrue(RegexValidator.validateCaptureGroupCount("(?P<test>.*)", 1).isOk());
  }
}
