package org.hypertrace.label.config.service;

import com.google.protobuf.Value;
import java.util.Optional;
import lombok.SneakyThrows;
import org.hypertrace.config.objectstore.IdentifiedObjectStore;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;
import org.hypertrace.config.service.change.event.api.ConfigChangeEventGenerator;
import org.hypertrace.config.service.v1.ConfigServiceGrpc;
import org.hypertrace.label.config.service.v1.Label;

public class LabelStore extends IdentifiedObjectStore<Label> {
  public static final String LABEL_CONFIG_RESOURCE_NAME = "label-config";
  public static final String LABELS_CONFIG_RESOURCE_NAMESPACE = "labels";

  protected LabelStore(
      ConfigServiceGrpc.ConfigServiceBlockingStub configServiceBlockingStub,
      ConfigChangeEventGenerator configChangeEventGenerator) {
    super(
        configServiceBlockingStub,
        LABELS_CONFIG_RESOURCE_NAMESPACE,
        LABEL_CONFIG_RESOURCE_NAME,
        configChangeEventGenerator);
  }

  @Override
  protected Optional<Label> buildDataFromValue(Value value) {
    try {
      Label.Builder builder = Label.newBuilder();
      ConfigProtoConverter.mergeFromValue(value, builder);
      return Optional.of(builder.build());
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  @Override
  @SneakyThrows
  protected Value buildValueFromData(Label object) {
    return ConfigProtoConverter.convertToValue(object);
  }

  @Override
  protected String getContextFromData(Label object) {
    return object.getId();
  }
}
