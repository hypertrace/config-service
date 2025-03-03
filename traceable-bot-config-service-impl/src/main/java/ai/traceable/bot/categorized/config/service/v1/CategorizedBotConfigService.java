package ai.traceable.bot.categorized.config.service.v1;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfigServiceGrpc.CategorizedBotConfigServiceImplBase;
import ai.traceable.bot.categorized.config.service.v1.validation.CategorizedBotConfigRequestValidator;
import com.google.inject.Inject;
import com.google.protobuf.ProtocolStringList;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;

@Slf4j
@RequiredArgsConstructor(onConstructor_ = {@Inject})
class CategorizedBotConfigService extends CategorizedBotConfigServiceImplBase {

  private final CategorizedBotDetailsConfig categorizedBotDetailsConfig;

  @Override
  public void getCategorizedBotConfigs(
      final GetCategorizedBotConfigsRequest request,
      final StreamObserver<GetCategorizedBotConfigsResponse> responseObserver) {
    // validate request
    final RequestContext requestContext = RequestContext.CURRENT.get();
    CategorizedBotConfigRequestValidator.validateRequestContext(requestContext);

    final List<CategorizedBotConfig> filteredBotList =
        categorizedBotDetailsConfig.getAllTraceableCategorizedBots().stream()
            .filter(
                bot ->
                    matchesFilter(request, CategorizedBotRequestFilter::getBotIdsList, bot.getId()))
            .filter(
                bot ->
                    matchesFilter(
                        request,
                        CategorizedBotRequestFilter::getCategoryList,
                        bot.getCategorizedBotDetails().getBotCategory()))
            .filter(
                bot ->
                    matchesFilter(
                        request,
                        CategorizedBotRequestFilter::getSubCategoryList,
                        bot.getCategorizedBotDetails().getBotSubCategory()))
            .collect(Collectors.toUnmodifiableList());
    responseObserver.onNext(
        GetCategorizedBotConfigsResponse.newBuilder().addAllBotConfigs(filteredBotList).build());
    responseObserver.onCompleted();
  }

  private boolean matchesFilter(
      final GetCategorizedBotConfigsRequest request,
      final Function<CategorizedBotRequestFilter, ProtocolStringList> listExtractor,
      final String value) {

    if (request.hasBotRequestFilter()) {
      final ProtocolStringList list = listExtractor.apply(request.getBotRequestFilter());
      return list.isEmpty() || list.contains(value);
    }
    return true;
  }
}
