package ai.traceable.fraud.policy.config.service.v1;

import ai.traceable.fraud.datamodel.config.service.v1.FieldType;
import ai.traceable.fraud.query.model.v1.DataQuery;
import ai.traceable.fraud.query.model.v1.EntityMetricDataQuery;
import ai.traceable.fraud.query.model.v1.MetricDataQuery;
import ai.traceable.fraud.query.model.v1.MetricQuery;
import ai.traceable.fraud.query.model.v1.TimeFrame;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FraudPolicyDefinitionsTest {
  private static final JsonFormat.Printer JSON_PRINTER = JsonFormat.printer();
  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();

  public static String serialize(Message m) {
    try {
      return JSON_PRINTER.print(m);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends Message.Builder> T deserialize(String value, T builder) {
    try {
      JSON_PARSER.merge(value, builder);
      return builder;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static void print(Message m) {
    System.out.println(serialize(m));
  }

  @Test
  void test_baseline() {
    MetricDataQuery lhs =
        MetricDataQuery.newBuilder()
            .setMetricQuery(
                MetricQuery.newBuilder()
                    .setMetricId("total_span_count")
                    .addGroupBy("api_id")
                    .addGroupBy("ip_org")
                    .setSingleAggregateValue(true)
                    .setTimeFrame(TimeFrame.newBuilder().setStartTime(1000).setEndTime(2000)))
            .build();
    FraudPolicy fraudPolicy =
        FraudPolicy.newBuilder()
            .setId("id1")
            .setName("num_registrations_by_email_domain")
            .setFraudPolicyRule(
                FraudPolicyRule.newBuilder()
                    .setFraudRule(
                        FraudRule.newBuilder()
                            .setMetricFraudRule(
                                MetricBasedFraudRule.newBuilder()
                                    .setMetricDataQuery(lhs)
                                    .setThresholdDefinition(
                                        ThresholdDefinition.newBuilder()
                                            .setThresholdOperator(
                                                ThresholdDefinition.ThresholdOperator
                                                    .THRESHOLD_OPERATOR_ABOVE)
                                            .setManualThreshold(
                                                ManualThreshold.newBuilder()
                                                    .setQueryBasedThreshold(
                                                        QueryBasedThreshold.newBuilder()
                                                            .setCopyQueryFromRule(true)
                                                            .setBaselineConfig(
                                                                BaselineConfig.newBuilder()
                                                                    .setBaselineDataConfig(
                                                                        BaselineDataConfig
                                                                            .newBuilder()
                                                                            .setSeasonality(
                                                                                BaselineDataConfig
                                                                                    .Seasonality
                                                                                    .SEASONALITY_DAILY)
                                                                            .setNumPeriods(7))
                                                                    .setBaselineSpec(
                                                                        BaselineSpec.newBuilder()
                                                                            .setEwmaBaseline(
                                                                                EWMABaseline
                                                                                    .getDefaultInstance())))))))))
            .build();
    print(fraudPolicy);
    FraudPolicy deserialized =
        deserialize(serialize(fraudPolicy), FraudPolicy.newBuilder()).build();
    Assertions.assertEquals(fraudPolicy, deserialized);
  }

  @Test
  void testBackreferencesInfo() {
    FraudPolicy policy =
        FraudPolicy.newBuilder()
            .setId("backref-test-id")
            .setName("policy_with_backreferences")
            // Add derivation config references
            .addBackreferencesInfo(
                BackreferencesInfo.newBuilder()
                    .setDerivationConfig(
                        BackreferencesInfo.DerivationConfigReference.newBuilder()
                            .setDerivationConfigId("derivation-config-123")))
            .addBackreferencesInfo(
                BackreferencesInfo.newBuilder()
                    .setDerivationConfig(
                        BackreferencesInfo.DerivationConfigReference.newBuilder()
                            .setDerivationConfigId("derivation-config-456")))
            .build();

    // Test serialization and deserialization
    FraudPolicy deserialized = deserialize(serialize(policy), FraudPolicy.newBuilder()).build();
    Assertions.assertEquals(policy, deserialized);

    // Verify backreferences content
    BackreferencesInfo derivationRef1 = policy.getBackreferencesInfo(0);
    BackreferencesInfo derivationRef2 = policy.getBackreferencesInfo(1);

    Assertions.assertEquals(
        "derivation-config-123", derivationRef1.getDerivationConfig().getDerivationConfigId());
    Assertions.assertEquals(
        "derivation-config-456", derivationRef2.getDerivationConfig().getDerivationConfigId());
  }

  @Test
  void test() {
    FraudPolicy policy =
        FraudPolicy.newBuilder()
            .setId("id1")
            .setName("email_domain_is_disposable_auto_threshold")
            .setFraudPolicyRule(
                FraudPolicyRule.newBuilder()
                    .setFraudRule(
                        FraudRule.newBuilder()
                            .setEntityMetricFraudRule(
                                EntityMetricBasedFraudRule.newBuilder()
                                    // assume this query fetches the accounts that are registered
                                    // from
                                    // disposable domains
                                    .setEntityMetricDataQuery(
                                        EntityMetricDataQuery.newBuilder()
                                            .setRawSqlQuery("select count(*) from some_table"))
                                    .setMessageFormat(
                                        "{$numAccounts} associated with {$numEmails} that have disposable domains")
                                    .putMessageParams("numAccounts", FieldType.FIELD_TYPE_LONG)
                                    .putMessageParams("numEmails", FieldType.FIELD_TYPE_LONG)
                                    .setThresholdDefinition(
                                        ThresholdDefinition.newBuilder()
                                            .setThresholdOperator(
                                                ThresholdDefinition.ThresholdOperator
                                                    .THRESHOLD_OPERATOR_ABOVE)
                                            // for automatic threshold.
                                            .setAutoThreshold(
                                                AutoThreshold.getDefaultInstance())))))
            .addBizImpactRules(
                FraudBizImpactRule.newBuilder()
                    .setFraudDataQuery(
                        FraudDataQuery.newBuilder()
                            // assume this query fetches the amount transferred out of accounts that
                            // have disposable emails
                            .setDataQuery(
                                DataQuery.newBuilder()
                                    .setMetricDataQuery(
                                        MetricDataQuery.newBuilder()
                                            .setRawSqlQuery("select something from sometable")))
                            .setMessageFormat("${$amount} transferred out of {$numAccounts}")
                            .putMessageParams("amount", FieldType.FIELD_TYPE_DOUBLE)
                            .putMessageParams("numAccounts", FieldType.FIELD_TYPE_LONG)))
            .setCategory(FraudPolicyCategory.FRAUD_POLICY_CATEGORY_RISK)
            .build();

    print(policy);
    FraudPolicy deserialized = deserialize(serialize(policy), FraudPolicy.newBuilder()).build();
    Assertions.assertEquals(policy, deserialized);
    policy =
        FraudPolicy.newBuilder()
            .setId("id2")
            .setName("email_domain_is_disposable_manual_threshold_scalar")
            .setFraudPolicyRule(
                FraudPolicyRule.newBuilder()
                    .setFraudRule(
                        FraudRule.newBuilder()
                            .setEntityMetricFraudRule(
                                EntityMetricBasedFraudRule.newBuilder()
                                    // assume this query fetches the accounts that are registered
                                    // from
                                    // disposable domains
                                    .setEntityMetricDataQuery(
                                        EntityMetricDataQuery.newBuilder()
                                            .setRawSqlQuery("select count(*) from some_table"))
                                    .setMessageFormat(
                                        "{$numAccounts} associated with {$numEmails} that have disposable domains")
                                    .putMessageParams("numAccounts", FieldType.FIELD_TYPE_LONG)
                                    .putMessageParams("numEmails", FieldType.FIELD_TYPE_LONG)
                                    .setThresholdDefinition(
                                        ThresholdDefinition.newBuilder()
                                            .setThresholdOperator(
                                                ThresholdDefinition.ThresholdOperator
                                                    .THRESHOLD_OPERATOR_ABOVE)
                                            // for manual threshold.
                                            .setManualThreshold(
                                                ManualThreshold.newBuilder().setStaticValue(10))))))
            .addBizImpactRules(
                FraudBizImpactRule.newBuilder()
                    .setFraudDataQuery(
                        FraudDataQuery.newBuilder()
                            // assume this query fetches the amount transferred out of accounts that
                            // have disposable emails
                            .setDataQuery(
                                DataQuery.newBuilder()
                                    .setMetricDataQuery(
                                        MetricDataQuery.newBuilder()
                                            .setRawSqlQuery("select something from sometable")))
                            .setMessageFormat("${$amount} transferred out of {$numAccounts}")
                            .putMessageParams("amount", FieldType.FIELD_TYPE_DOUBLE)
                            .putMessageParams("numAccounts", FieldType.FIELD_TYPE_LONG)))
            .build();
    print(policy);
    deserialized = deserialize(serialize(policy), FraudPolicy.newBuilder()).build();
    Assertions.assertEquals(policy, deserialized);

    policy =
        FraudPolicy.newBuilder()
            .setId("id2")
            .setName("email_domain_is_disposable_manual_threshold_baseline")
            .setFraudPolicyRule(
                FraudPolicyRule.newBuilder()
                    .setFraudRule(
                        FraudRule.newBuilder()
                            .setEntityMetricFraudRule(
                                EntityMetricBasedFraudRule.newBuilder()
                                    // assume this query fetches the accounts that are registered
                                    // from
                                    // disposable domains
                                    .setEntityMetricDataQuery(
                                        EntityMetricDataQuery.newBuilder()
                                            .setRawSqlQuery("select something from sometable"))
                                    .setMessageFormat(
                                        "{$numAccounts} associated with {$numEmails} that have disposable domains")
                                    .putMessageParams("numAccounts", FieldType.FIELD_TYPE_LONG)
                                    .putMessageParams("numEmails", FieldType.FIELD_TYPE_LONG)
                                    .setThresholdDefinition(
                                        ThresholdDefinition.newBuilder()
                                            .setThresholdOperator(
                                                ThresholdDefinition.ThresholdOperator
                                                    .THRESHOLD_OPERATOR_ABOVE)
                                            // for manual threshold.
                                            .setManualThreshold(
                                                ManualThreshold.newBuilder()
                                                    .setQueryBasedThreshold(
                                                        QueryBasedThreshold.newBuilder()
                                                            .setCopyQueryFromRule(true)
                                                            .setBaselineConfig(
                                                                BaselineConfig.newBuilder()
                                                                    .setBaselineDataConfig(
                                                                        BaselineDataConfig
                                                                            .newBuilder()
                                                                            .setSeasonality(
                                                                                BaselineDataConfig
                                                                                    .Seasonality
                                                                                    .SEASONALITY_DAILY)
                                                                            .setNumPeriods(30))
                                                                    .setBaselineSpec(
                                                                        BaselineSpec.newBuilder()
                                                                            .setSimpleAggregationBaseline(
                                                                                SimpleAggregationBaseline
                                                                                    .newBuilder()
                                                                                    .setQuantileFunc(
                                                                                        QuantileFunc
                                                                                            .newBuilder()
                                                                                            .setQuantile(
                                                                                                0.75)))))))))))
            .addBizImpactRules(
                FraudBizImpactRule.newBuilder()
                    .setFraudDataQuery(
                        FraudDataQuery.newBuilder()
                            // assume this query fetches the amount transferred out of accounts that
                            // have disposable emails
                            .setDataQuery(
                                DataQuery.newBuilder()
                                    .setMetricDataQuery(
                                        MetricDataQuery.newBuilder()
                                            .setRawSqlQuery("select something from sometable")))
                            .setMessageFormat("${$amount} transferred out of {$numAccounts}")
                            .putMessageParams("amount", FieldType.FIELD_TYPE_DOUBLE)
                            .putMessageParams("numAccounts", FieldType.FIELD_TYPE_LONG)))
            .setCategory(FraudPolicyCategory.FRAUD_POLICY_CATEGORY_RISK)
            .build();
    print(policy);
    deserialized = deserialize(serialize(policy), FraudPolicy.newBuilder()).build();
    Assertions.assertEquals(policy, deserialized);
  }
}
