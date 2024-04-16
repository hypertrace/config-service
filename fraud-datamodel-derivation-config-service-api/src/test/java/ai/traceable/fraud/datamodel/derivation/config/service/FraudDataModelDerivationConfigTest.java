package ai.traceable.fraud.datamodel.derivation.config.service;

import ai.traceable.fraud.datamodel.derivation.config.service.v1.DerivationConfig;
import com.google.common.io.Resources;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class FraudDataModelDerivationConfigTest {
  private static final JsonFormat.Printer jsonPrinter =
      JsonFormat.printer().includingDefaultValueFields();
  private static final JsonFormat.Parser jsonParser = JsonFormat.parser();

  public static String serialize(Message m) {
    try {
      return jsonPrinter.print(m);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends Message.Builder> T deserialize(String value, T builder) {
    try {
      jsonParser.merge(value, builder);
      return builder;
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testSerDe() throws IOException {
    var datasetDerivationConfig =
        derivationConfig("dataset_derivation_1", "dataset_derivation.yaml");
    var entityDerivationConfig = derivationConfig("entity_derivation_1", "entity_derivation.yaml");
    var relationshipDerivationConfig =
        derivationConfig("relationship_derivation_1", "relationship_derivation.yaml");
    Assertions.assertNotNull(datasetDerivationConfig);
    Assertions.assertNotNull(entityDerivationConfig);
    Assertions.assertNotNull(relationshipDerivationConfig);

    var serialized = serialize(datasetDerivationConfig);
    var deserialized = deserialize(serialized, DerivationConfig.newBuilder()).build();
    Assertions.assertEquals(datasetDerivationConfig, deserialized);

    serialized = serialize(datasetDerivationConfig);
    deserialized = deserialize(serialized, DerivationConfig.newBuilder()).build();
    Assertions.assertEquals(datasetDerivationConfig, deserialized);

    serialized = serialize(datasetDerivationConfig);
    deserialized = deserialize(serialized, DerivationConfig.newBuilder()).build();
    Assertions.assertEquals(datasetDerivationConfig, deserialized);
  }

  private DerivationConfig derivationConfig(String configId, String yamlResourcePath)
      throws IOException {
    URL resource = Resources.getResource(yamlResourcePath);
    String yamlPayload = Resources.toString(resource, Charset.defaultCharset());
    return DerivationConfig.newBuilder()
        .setDerivationConfigId(configId)
        .setName(configId)
        .setDerivationConfig(yamlPayload)
        .build();
  }
}
