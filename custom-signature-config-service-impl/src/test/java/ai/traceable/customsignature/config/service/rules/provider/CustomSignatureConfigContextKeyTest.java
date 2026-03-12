package ai.traceable.customsignature.config.service.rules.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import ai.traceable.customsignature.config.service.v1.EventType;
import ai.traceable.customsignature.config.service.v1.GetCustomSignatureEvaluationConfigContextRequest;
import ai.traceable.customsignature.config.service.v1.RuleEvaluationPoint;
import org.junit.jupiter.api.Test;

class CustomSignatureConfigContextKeyTest {

  @Test
  void testEqualsWithSameRuleEvaluationPointAndEventType() {
    GetCustomSignatureEvaluationConfigContextRequest request1 =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    GetCustomSignatureEvaluationConfigContextRequest request2 =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    CustomSignatureConfigContextKey key1 = CustomSignatureConfigContextKey.from(request1);
    CustomSignatureConfigContextKey key2 = CustomSignatureConfigContextKey.from(request2);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  void testEqualsWithSameRuleEvaluationPointDifferentEventType() {
    GetCustomSignatureEvaluationConfigContextRequest request1 =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    GetCustomSignatureEvaluationConfigContextRequest request2 =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .build();

    CustomSignatureConfigContextKey key1 = CustomSignatureConfigContextKey.from(request1);
    CustomSignatureConfigContextKey key2 = CustomSignatureConfigContextKey.from(request2);

    assertNotEquals(key1, key2);
    assertNotEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  void testEqualsWithDifferentRuleEvaluationPointSameEventType() {
    GetCustomSignatureEvaluationConfigContextRequest request1 =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    GetCustomSignatureEvaluationConfigContextRequest request2 =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    CustomSignatureConfigContextKey key1 = CustomSignatureConfigContextKey.from(request1);
    CustomSignatureConfigContextKey key2 = CustomSignatureConfigContextKey.from(request2);

    assertNotEquals(key1, key2);
    assertNotEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  void testEqualsWithDifferentRuleEvaluationPointAndEventType() {
    GetCustomSignatureEvaluationConfigContextRequest request1 =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    GetCustomSignatureEvaluationConfigContextRequest request2 =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_PLATFORM)
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .build();

    CustomSignatureConfigContextKey key1 = CustomSignatureConfigContextKey.from(request1);
    CustomSignatureConfigContextKey key2 = CustomSignatureConfigContextKey.from(request2);

    assertNotEquals(key1, key2);
    assertNotEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  void testEqualsWithSameObject() {
    GetCustomSignatureEvaluationConfigContextRequest request =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    CustomSignatureConfigContextKey key = CustomSignatureConfigContextKey.from(request);

    assertEquals(key, key);
    assertEquals(key.hashCode(), key.hashCode());
  }

  @Test
  void testEqualsWithNull() {
    GetCustomSignatureEvaluationConfigContextRequest request =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    CustomSignatureConfigContextKey key = CustomSignatureConfigContextKey.from(request);

    assertNotEquals(null, key);
  }

  @Test
  void testGetRequest() {
    GetCustomSignatureEvaluationConfigContextRequest request =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    CustomSignatureConfigContextKey key = CustomSignatureConfigContextKey.from(request);

    assertEquals(request, key.getRequest());
  }

  @Test
  void testFromMethod() {
    GetCustomSignatureEvaluationConfigContextRequest request =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    CustomSignatureConfigContextKey key1 = CustomSignatureConfigContextKey.from(request);
    CustomSignatureConfigContextKey key2 = new CustomSignatureConfigContextKey(request);

    assertEquals(key1, key2);
    assertEquals(key1.hashCode(), key2.hashCode());
  }

  @Test
  void testHashCodeConsistency() {
    GetCustomSignatureEvaluationConfigContextRequest request =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    CustomSignatureConfigContextKey key = CustomSignatureConfigContextKey.from(request);

    // Hash code should be consistent across multiple calls
    int hashCode1 = key.hashCode();
    int hashCode2 = key.hashCode();
    int hashCode3 = key.hashCode();

    assertEquals(hashCode1, hashCode2);
    assertEquals(hashCode2, hashCode3);
  }

  @Test
  void testCacheKeyIsolation() {
    // Test that different event types create different cache keys
    // This is critical for proper caching behavior

    GetCustomSignatureEvaluationConfigContextRequest allowRequest =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_ALLOW)
            .build();

    GetCustomSignatureEvaluationConfigContextRequest blockRequest =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_DETECTION_AND_BLOCKING)
            .build();

    GetCustomSignatureEvaluationConfigContextRequest normalRequest =
        GetCustomSignatureEvaluationConfigContextRequest.newBuilder()
            .setRuleEvaluationPoint(RuleEvaluationPoint.RULE_EVALUATION_POINT_EDGE)
            .setEventType(EventType.EVENT_TYPE_NORMAL_DETECTION)
            .build();

    CustomSignatureConfigContextKey allowKey = CustomSignatureConfigContextKey.from(allowRequest);
    CustomSignatureConfigContextKey blockKey = CustomSignatureConfigContextKey.from(blockRequest);
    CustomSignatureConfigContextKey normalKey = CustomSignatureConfigContextKey.from(normalRequest);

    // All keys should be different
    assertNotEquals(allowKey, blockKey);
    assertNotEquals(allowKey, normalKey);
    assertNotEquals(blockKey, normalKey);

    // All hash codes should be different
    assertNotEquals(allowKey.hashCode(), blockKey.hashCode());
    assertNotEquals(allowKey.hashCode(), normalKey.hashCode());
    assertNotEquals(blockKey.hashCode(), normalKey.hashCode());
  }
}
