package ai.traceable.sensitivedata.config.service;

import static ai.traceable.sensitivedata.config.service.SensitiveDataConfigUtils.toValue;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_HASH;
import static ai.traceable.sensitivedata.config.service.v1.RedactionStrategy.REDACTION_STRATEGY_REDACT;

import ai.traceable.sensitivedata.config.service.v1.ParamType;
import ai.traceable.sensitivedata.config.service.v1.Parameter;
import ai.traceable.sensitivedata.config.service.v1.ParameterWithSensitivity;
import ai.traceable.sensitivedata.config.service.v1.PiiElement;
import ai.traceable.sensitivedata.config.service.v1.PiiFilterConfig;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;

public class TestUtils {

  static final String TENANT_ID = "tenant1";
  static final String ENDPOINT1 = "/checkout";
  static final String ENDPOINT2 = "/orders";

  public static PiiFilterConfig getPiiFilterConfigInstance() {
    PiiElement piiElement1 =
        PiiElement.newBuilder()
            .setRegex("p1")
            .setRedactionStrategy(REDACTION_STRATEGY_REDACT)
            .build();
    PiiElement piiElement2 =
        PiiElement.newBuilder()
            .setRegex("p2")
            .setRedactionStrategy(REDACTION_STRATEGY_HASH)
            .build();
    return PiiFilterConfig.newBuilder().addAllKeyRegexs(List.of(piiElement1, piiElement2)).build();
  }

  public static Value getPiiFilterConfigValue() {
    ListValue keyRegexsList =
        ListValue.newBuilder()
            .addValues(getKeyRegexValue("p1", "REDACTION_STRATEGY_REDACT"))
            .addValues(getKeyRegexValue("p2", "REDACTION_STRATEGY_HASH"))
            .build();
    Value keyRegexs = Value.newBuilder().setListValue(keyRegexsList).build();
    Struct struct = Struct.newBuilder().putFields("keyRegexs", keyRegexs).build();
    Value piiFilterConfigValue = Value.newBuilder().setStructValue(struct).build();
    return piiFilterConfigValue;
  }

  public static Value getKeyRegexValue(String paramName, String redactionStrategy) {
    Struct struct =
        Struct.newBuilder()
            .putFields("regex", Value.newBuilder().setStringValue(paramName).build())
            .putFields(
                "redactionStrategy", Value.newBuilder().setStringValue(redactionStrategy).build())
            .build();
    return Value.newBuilder().setStructValue(struct).build();
  }

  public static Parameter getParameter(ParamType paramType, String paramName) {
    return Parameter.newBuilder().setParamType(paramType).setName(paramName).build();
  }

  public static ParameterWithSensitivity getParameterWithSensitivity(
      Parameter parameter, boolean isSensitive) {
    return ParameterWithSensitivity.newBuilder()
        .setParameter(parameter)
        .setSensitive(isSensitive)
        .build();
  }

  public static List<ParameterWithSensitivity> getParametersWithSensitivityList() {
    Parameter parameter1 = getParameter(ParamType.PARAM_TYPE_BODY, "p1");
    Parameter parameter2 = getParameter(ParamType.PARAM_TYPE_BODY, "p2");
    Parameter parameter3 = getParameter(ParamType.PARAM_TYPE_BODY, "p3");
    ParameterWithSensitivity parameterWithSensitivity1 =
        getParameterWithSensitivity(parameter1, true);
    ParameterWithSensitivity parameterWithSensitivity2 =
        getParameterWithSensitivity(parameter2, false);
    ParameterWithSensitivity parameterWithSensitivity3 =
        getParameterWithSensitivity(parameter3, true);
    return List.of(parameterWithSensitivity1, parameterWithSensitivity2, parameterWithSensitivity3);
  }

  public static Value getParametersWithSensitivityValue() {
    try {
      ListValue.Builder builder = ListValue.newBuilder();
      for (ParameterWithSensitivity parameterWithSensitivity : getParametersWithSensitivityList()) {
        builder.addValues(toValue(parameterWithSensitivity));
      }
      return Value.newBuilder().setListValue(builder.build()).build();
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException("Exception while generating parametersWithSensitivityValue", e);
    }
  }
}
