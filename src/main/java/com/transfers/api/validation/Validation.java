package com.transfers.api.validation;

import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.validation.CustomValidator;
import io.vertx.ext.web.api.validation.HTTPRequestValidationHandler;
import io.vertx.ext.web.api.validation.ParameterType;
import io.vertx.ext.web.api.validation.ValidationException;

public class Validation {

    public static HTTPRequestValidationHandler newAccountValidationHandler() {
        return HTTPRequestValidationHandler.create()
                .addJsonBodySchema("{\"type\":\"object\",\"properties\":{\"name\":{\"type\":\"string\",\"minLength\":2}},\"required\":[\"name\"]}");
    }

    public static HTTPRequestValidationHandler getAccountValidationHandler() {
        return HTTPRequestValidationHandler.create()
                .addPathParam("id", ParameterType.INT);
    }

    public static HTTPRequestValidationHandler balanceValidationHandler() {
        return HTTPRequestValidationHandler.create()
                .addPathParam("id", ParameterType.INT)
                .addCustomValidatorFunction(new BalanceOperationValidator())
                .addJsonBodySchema("{\"type\":\"object\",\"properties\":{\"operation\":{\"type\":\"string\",\"minLength\":7,\"maxLength\":8},\"amount\":{\"type\":\"number\",\"multipleOf\":1.0,\"minimum\":1}},\"required\":[\"operation\",\"amount\"]}");
    }

    public static HTTPRequestValidationHandler transferValidationHandler() {
        return HTTPRequestValidationHandler.create()
                .addCustomValidatorFunction(new TransferValidator())
                .addJsonBodySchema("{\"type\":\"object\",\"properties\":{\"amount\":{\"type\":\"number\",\"multipleOf\":1.0,\"minimum\":1},\"senderAccountId\":{\"type\":\"number\",\"minimum\":1},\"receiverAccountId\":{\"type\":\"number\",\"minimum\":1}},\"required\":[\"senderAccountId\",\"receiverAccountId\",\"amount\"]}");
    }

    private static class BalanceOperationValidator implements CustomValidator {
        @Override
        public void validate(RoutingContext rc) throws ValidationException {
            String operation = rc.getBodyAsJson().getString("operation");
            if (operation != null && !operation.equals("withdraw") && !operation.equals("deposit")) {
                throw new ValidationException("Operation must be or 'deposit' or 'withdraw'");
            }
        }
    }

    private static class TransferValidator implements CustomValidator {
        @Override
        public void validate(RoutingContext rc) throws ValidationException {
            JsonObject transferJsonObj = rc.getBodyAsJson();
            Long senderAccountId = transferJsonObj.getLong("senderAccountId");
            Long receiverAccountId = transferJsonObj.getLong("receiverAccountId");
            if (senderAccountId != null && senderAccountId.equals(receiverAccountId)) {
                throw new ValidationException("Sender and receiver accounts must be different");
            }
        }
    }
}
