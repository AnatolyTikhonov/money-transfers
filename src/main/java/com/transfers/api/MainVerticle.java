package com.transfers.api;

import com.transfers.api.validation.Validation;
import io.vertx.core.*;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.RequestParameters;
import io.vertx.ext.web.api.validation.ValidationException;
import io.vertx.ext.web.handler.BodyHandler;

import java.time.Instant;

import static com.transfers.api.model.Address.*;

public class MainVerticle extends AbstractVerticle {

    @Override
    public void start() {
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());
        vertx.deployVerticle(RepositoryVerticle.class.getName());
        vertx.createHttpServer()
                .requestHandler(router)
                .listen(config().getInteger("http.port", 8888));

        // creating new account
        router.route().method(HttpMethod.POST).path("/accounts")
                .handler(Validation.newAccountValidationHandler())
                .handler(rc -> {
                    JsonObject account = rc.getBodyAsJson();
                    vertx.eventBus().request(NEW_ACCOUNT_ADDR, account, handleResponse(rc, 201));
                });

        // get account
        router.route().method(HttpMethod.GET).path("/accounts/:id")
                .handler(Validation.getAccountValidationHandler())
                .handler(rc -> {
                    Long id = ((RequestParameters) rc.get("parsedParameters")).pathParameter("id").getInteger().longValue();
                    vertx.eventBus().request(ACCOUNT_ADDR, id, handleResponse(rc, 200));
                });

        // user performs balance operations: deposit or withdraw money
        router.route().method(HttpMethod.POST).path("/accounts/:id/balance")
                .handler(Validation.balanceValidationHandler())
                .handler(rc -> {
                    Long id = ((RequestParameters) rc.get("parsedParameters")).pathParameter("id").getInteger().longValue();
                    JsonObject balanceOperationJsonObj = rc.getBodyAsJson()
                            .put("accountId", id);
                    vertx.eventBus().request(BALANCE_OPERATION_ADDR, balanceOperationJsonObj, handleResponse(rc, 200));
                });

        // get current balance
        router.route().method(HttpMethod.GET).path("/accounts/:id/balance")
                .handler(Validation.getAccountValidationHandler())
                .handler(rc -> {
                    Long accountId = ((RequestParameters) rc.get("parsedParameters")).pathParameter("id").getInteger().longValue();
                    vertx.eventBus().request(BALANCE_ADDR, accountId, handleResponse(rc, 200));
                });

        // perform transfer
        router.route().method(HttpMethod.POST).path("/transfers")
                .handler(Validation.transferValidationHandler())
                .handler(rc -> {
                    JsonObject transferJsonObj = rc.getBodyAsJson()
                            .put("operation", "transfer");
                    vertx.eventBus().request(NEW_TRANSFER_ADDR, transferJsonObj, handleResponse(rc, 200));
                });

        // get transactions related to specific account
        router.route().method(HttpMethod.GET).path("/accounts/:id/transactions")
                .handler(Validation.getAccountValidationHandler())
                .handler(rc -> {
                    Long accountId = ((RequestParameters) rc.get("parsedParameters")).pathParameter("id").getInteger().longValue();
                    vertx.eventBus().request(TRANSACTIONS_ADDR, accountId, handleResponse(rc, 200));
                });

        router.errorHandler(400, rc -> {
            if (rc.failure() instanceof ValidationException) {
                // Something went wrong during validation!
                replyWithError(400, rc.failure(), rc);
            } else {
                // Unknown 400 failure happened
                rc.response().setStatusCode(400).end();
            }
        });
    }

    private void replyWithError(Integer status, Throwable error, RoutingContext rc) {
        JsonObject response = new JsonObject()
                .put("error", "Failure on " + rc.normalisedPath() + ", reason: " + error.getMessage());
        rc.response()
                .putHeader("content-type", "application/json")
                .setChunked(true)
                .setStatusCode(status)
                .write(response.toBuffer())
                .end();
    }

    private void replyWithBody(Integer status, Object responseBody, RoutingContext rc) {
        JsonObject response = new JsonObject()
                .put("data", responseBody)
                .put("timestamp", Instant.now().getEpochSecond());
        rc.response()
                .putHeader("content-type", "application/json")
                .setChunked(true)
                .setStatusCode(status)
                .write(response.toBuffer())
                .end();
    }

    private <T>Handler<AsyncResult<Message<T>>> handleResponse(RoutingContext rc, Integer successStatus) {
        return resp -> {
            if (resp.succeeded()) {
                replyWithBody(successStatus, resp.result().body(), rc);
            } else {
                ReplyException cause = (ReplyException) resp.cause();
                replyWithError(cause.failureCode(), resp.cause(), rc);
            }
        };
    }

    public static void main(final String[] args) {
        Launcher.executeCommand("run", MainVerticle.class.getName());
    }
}
