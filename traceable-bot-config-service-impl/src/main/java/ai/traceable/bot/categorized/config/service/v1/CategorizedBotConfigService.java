package ai.traceable.bot.categorized.config.service.v1;

import ai.traceable.bot.categorized.config.service.v1.CategorizedBotConfigServiceGrpc.CategorizedBotConfigServiceImplBase;
import ai.traceable.bot.categorized.config.service.v1.translator.CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator;
import ai.traceable.bot.categorized.config.service.v1.validation.CategorizedBotConfigRequestValidator;
import com.google.inject.Inject;
import com.google.protobuf.ProtocolStringList;
import io.grpc.stub.StreamObserver;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;

@RequiredArgsConstructor(onConstructor_ = @Inject)
public class CategorizedBotConfigService extends CategorizedBotConfigServiceImplBase {

  @Override
  public void getCategorizedBotCategoryDetails(
      final GetCategorizedBotCategoryDetailsRequest request,
      final StreamObserver<GetCategorizedBotCategoryDetailsResponse> responseObserver) {
    // validate request
    final RequestContext requestContext = RequestContext.CURRENT.get();
    CategorizedBotConfigRequestValidator.validateRequestContext(requestContext);

    final Collection<BotCategory> botCategories;
    if (request.getBotCategoryIdsList().isEmpty()) {
      botCategories = CategorizedBotCategoriesConfig.INSTANCE.getBotCategories();
    } else {
      botCategories =
          CategorizedBotCategoriesConfig.INSTANCE.getBotCategories().stream()
              .filter(botCategory -> request.getBotCategoryIdsList().contains(botCategory.getId()))
              .collect(Collectors.toUnmodifiableList());
    }
    responseObserver.onNext(
        GetCategorizedBotCategoryDetailsResponse.newBuilder()
            .addAllBotCategories(botCategories)
            .build());
    responseObserver.onCompleted();
  }

  @Override
  public void getCategorizedBotConfigs(
      final GetCategorizedBotConfigsRequest request,
      final StreamObserver<GetCategorizedBotConfigsResponse> responseObserver) {
    // validate request
    final RequestContext requestContext = RequestContext.CURRENT.get();
    CategorizedBotConfigRequestValidator.validateRequestContext(requestContext);

    final List<CategorizedBotConfig> filteredBotList =
        CategorizedBotDetailsConfig.INSTANCE.getAllTraceableCategorizedBots().stream()
            .filter(
                bot ->
                    matchesFilter(request, CategorizedBotRequestFilter::getBotIdsList, bot.getId()))
            .filter(
                bot ->
                    matchesFilter(
                        request,
                        CategorizedBotRequestFilter::getBotCategoryIdsList,
                        bot.getCategorizedBotDetails().getBotCategoryId()))
            .filter(
                bot ->
                    matchesFilter(
                        request,
                        CategorizedBotRequestFilter::getBotSubCategoryIdsList,
                        bot.getCategorizedBotDetails().getBotSubCategoryId()))
            .collect(Collectors.toUnmodifiableList());
    responseObserver.onNext(
        GetCategorizedBotConfigsResponse.newBuilder().addAllBotConfigs(filteredBotList).build());
    responseObserver.onCompleted();
  }

  @Override
  public void getCategorizedBotConfigEdgeDecisionVariables(
      final GetCategorizedBotConfigEdgeDecisionVariablesRequest request,
      final StreamObserver<GetCategorizedBotConfigEdgeDecisionVariablesResponse> responseObserver) {
    // validate request
    final RequestContext requestContext = RequestContext.CURRENT.get();
    CategorizedBotConfigRequestValidator.validateRequestContext(requestContext);
    responseObserver.onNext(
        GetCategorizedBotConfigEdgeDecisionVariablesResponse.newBuilder()
            .addVariableDerivationMappings(
                CategorizedBotConfigDetailsToEdgeDecisionVariablesTranslator.INSTANCE.translate(
                    CategorizedBotDetailsConfig.INSTANCE.getAllTraceableCategorizedBots().stream()
                        .map(CategorizedBotConfig::getId)
                        .collect(Collectors.toUnmodifiableList())))
            .build());
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
