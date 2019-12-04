package com.transfers.api;

import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

import java.time.Instant;
import java.util.Arrays;

import static com.transfers.api.model.Address.*;

public class RepositoryVerticle extends AbstractVerticle {

    private static final String ACCOUNT_PROFILE_FORMAT = "account:%s:profile";
    private static final String ACCOUNTS_MAP = "accounts";
    private static final String TRANSACTIONS_MAP = "transactions";

    @Override
    public void start() {
        vertx.eventBus().consumer(NEW_ACCOUNT_ADDR, message -> {
            vertx.sharedData().getCounter("account", counter -> counter.result().incrementAndGet(count -> {
                Long accountId = count.result();
                JsonObject account = ((JsonObject) message.body())
                        .put("id", accountId)
                        .put("balance", 0);
                saveAccount(accountId, account);
                message.reply(accountId);
            }));
        });

        vertx.eventBus().consumer(ACCOUNT_ADDR, message -> {
            Long accountId = (Long) message.body();
            if (accountNotExists(accountId)) {
                message.fail(404, "Account not found");
            } else {
                message.reply(getAccount(accountId));
            }
        });

        vertx.eventBus().consumer(BALANCE_OPERATION_ADDR, message -> {
            JsonObject balanceOperationJsonObj = (JsonObject) message.body();
            Long accountId = balanceOperationJsonObj.getLong("accountId");
            if (accountNotExists(accountId)) {
                message.fail(404, "Account not found");
            } else if (notEnoughFunds(accountId, balanceOperationJsonObj)) {
                message.fail(422, "Not enough funds");
            } else {
                saveTransaction(message, balanceOperationJsonObj, accountId);
            }
        });

        vertx.eventBus().consumer(BALANCE_ADDR, message -> {
            Long accountId = (Long) message.body();
            if (accountNotExists(accountId)) {
                message.fail(404, "Account not found");
            } else {
                JsonObject account = (JsonObject) accounts().get(String.format(ACCOUNT_PROFILE_FORMAT, accountId));
                message.reply(account.getLong("balance"));
            }
        });

        vertx.eventBus().consumer(NEW_TRANSFER_ADDR, message -> {
            JsonObject transferRequest = (JsonObject) message.body();
            Long senderAccountId = transferRequest.getLong("senderAccountId");
            Long receiverAccountId = transferRequest.getLong("receiverAccountId");
            if (accountNotExists(senderAccountId) || accountNotExists(receiverAccountId)) {
                message.fail(404, "Account not found");
            } else if (notEnoughFunds(senderAccountId, transferRequest)) {
                message.fail(422, "Not enough funds");
            } else {
                saveTransaction(message, transferRequest, senderAccountId, receiverAccountId);
            }
        });

        vertx.eventBus().consumer(TRANSACTIONS_ADDR, message -> {
            Long accountId = (Long) message.body();
            if (accountNotExists(accountId)) {
                message.fail(404, "Account not found");
            } else {
                vertx.sharedData().getCounter("transaction", counter -> counter.result().get(count -> {
                    LocalMap<Object, Object> transactionsMap = transactions();
                    Long lastId = count.result();
                    Flowable.rangeLong(1, lastId)
                            .map(i -> lastId - i + 1)
                            .map(transactionsMap::get)
                            .cast(JsonObject.class)
                            .filter(transaction -> this.belongsToAccount(transaction, accountId))
                            .map(transaction -> toHistoryRow(transaction, accountId))
                            .reduce(new JsonArray(), JsonArray::add)
                            .subscribe((Consumer<JsonArray>) message::reply);
                }));
            }
        });
    }

    private boolean notEnoughFunds(Long accountId, JsonObject balanceOperationJsonObj) {
        JsonObject account = getAccount(accountId);
        Integer effectiveAmmount = effectiveAmount(balanceOperationJsonObj, accountId);
        return (effectiveAmmount < 0 && account.getLong("balance") < Math.abs(effectiveAmmount));
    }

    private void saveTransaction(Message<Object> message, JsonObject transactionJsonObj, Long... relatedAccountIds) {
        vertx.sharedData().getCounter("transaction", counter -> counter.result().incrementAndGet(count -> {
            Long transactionId = count.result();
            transactionJsonObj.put("id", transactionId);
            transactionJsonObj.put("timestamp", Instant.now().getEpochSecond());
            transactions().put(transactionId, transactionJsonObj);
            updateBalance(transactionJsonObj, relatedAccountIds);
            message.reply(transactionId);
        }));
    }

    private void updateBalance(JsonObject transactionJsonObj, Long... relatedAccountIds) {
        Arrays.stream(relatedAccountIds)
                .forEach(accountId -> {
                    Integer effectiveAmount = effectiveAmount(transactionJsonObj, accountId);
                    JsonObject account = getAccount(accountId);
                    account.put("balance", account.getLong("balance") + effectiveAmount);
                    saveAccount(accountId, account);
                });
    }

    private boolean belongsToAccount(JsonObject transaction, Long accountId) {
        return accountId.equals(transaction.getLong("accountId")) ||
                accountId.equals(transaction.getLong("senderAccountId")) ||
                accountId.equals(transaction.getLong("receiverAccountId"));
    }

    private LocalMap<Object, Object> transactions() {
        return vertx.sharedData().getLocalMap(TRANSACTIONS_MAP);
    }

    private LocalMap<Object, Object> accounts() {
        return vertx.sharedData().getLocalMap(ACCOUNTS_MAP);
    }

    private boolean accountNotExists(Long accountId) {
        return !accounts().containsKey(String.format(ACCOUNT_PROFILE_FORMAT, accountId));
    }

    private JsonObject getAccount(Long accountId) {
        return (JsonObject) accounts().get(String.format(ACCOUNT_PROFILE_FORMAT, accountId));
    }

    private void saveAccount(Long accountId, JsonObject account) {
        accounts().put(String.format(ACCOUNT_PROFILE_FORMAT, accountId), account);
    }

    private Integer effectiveAmount(JsonObject transactionJsonObj, Long accountId) {
        Integer amount = transactionJsonObj.getInteger("amount");
        String operation = transactionJsonObj.getString("operation");
        Long senderAccountId = transactionJsonObj.getLong("senderAccountId");
        if ("withdraw".equals(operation) || accountId.equals(senderAccountId)) {
            amount = -amount;
        }
        return amount;
    }

    private JsonObject toHistoryRow(JsonObject transaction, Long accountId) {
        return new JsonObject()
                .put("id", transaction.getLong("id"))
                .put("timestamp", transaction.getLong("timestamp"))
                .put("operation", transaction.getString("operation"))
                .put("amount", effectiveAmount(transaction, accountId));
    }
}
