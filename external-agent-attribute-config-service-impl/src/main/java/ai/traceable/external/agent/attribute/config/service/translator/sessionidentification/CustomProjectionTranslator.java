package ai.traceable.external.agent.attribute.config.service.translator.sessionidentification;

import ai.traceable.external.agent.attribute.config.service.v1.AttributeRule.Projector;
import ai.traceable.sessionidentification.config.service.v1.CustomProjection;
import com.google.protobuf.InvalidProtocolBufferException;
import io.grpc.Status;
import org.hypertrace.config.proto.converter.ConfigProtoConverter;

class CustomProjectionTranslator {
  Projector translateCustomProjection(CustomProjection customProjection) {
    Projector.Builder projector = Projector.newBuilder();
    try {
      ConfigProtoConverter.mergeFromJsonString(customProjection.getCustomProjection(), projector);
    } catch (InvalidProtocolBufferException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription(
              String.format(
                  "Invalid custom projection json %s", customProjection.getCustomProjection()))
          .asRuntimeException();
    }
    return projector.build();
  }
}
