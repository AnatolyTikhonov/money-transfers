package com.transfers.api;

import com.transfers.api.util.Operation;
import io.reactivex.Flowable;
import io.reactivex.functions.Consumer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;

import java.time.Instant;
import java.util.Arrays;

import static com.transfers.api.util.Address.*;
import static com.transfers.api.util.Consts.*;

public class RepositoryVerticle extends AbstractVerticle {

    private static final String ACCOUNTS_MAP = "accounts";
    private static final String TRANSACTIONS_MAP = "transactions";

    private static final String ACCOUNTS_COUNTER = "account";
    private static final String TRANSACTIONS_COUNTER = "transaction";


    @Override
    public void start() {
        vertx.eventBus().consumer(NEW_ACCOUNT_ADDR, message -> {
            vertx.sharedData().getCounter(ACCOUNTS_COUNTER, counter -> counter.result().incrementAndGet(count -> {
                Long accountId = count.result();
                JsonObject account = ((JsonObject) message.body())
                        .put(ID, accountId)
                        .put(BALANCE, 0);
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
            Long accountId = balanceOperationJsonObj.getLong(ACCOUNT_ID);
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
                JsonObject account = (JsonObject) accounts().get(accountId);
                message.reply(account.getLong(BALANCE));
            }
        });

        vertx.eventBus().consumer(NEW_TRANSFER_ADDR, message -> {
            JsonObject transferRequest = (JsonObject) message.body();
            Long senderAccountId = transferRequest.getLong(SENDER_ACCOUNT_ID);
            Long receiverAccountId = transferRequest.getLong(RECEIVER_ACCOUNT_ID);
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
                vertx.sharedData().getCounter(TRANSACTIONS_COUNTER, counter -> counter.result().get(count -> {
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
        return (effectiveAmmount < 0 && account.getLong(BALANCE) < Math.abs(effectiveAmmount));
    }

    private void saveTransaction(Message<Object> message, JsonObject transactionJsonObj, Long... relatedAccountIds) {
        vertx.sharedData().getCounter(TRANSACTIONS_COUNTER, counter -> counter.result().incrementAndGet(count -> {
            Long transactionId = count.result();
            transactionJsonObj.put(ID, transactionId);
            transactionJsonObj.put(TIMESTAMP, Instant.now().getEpochSecond());
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
                    account.put(BALANCE, account.getLong(BALANCE) + effectiveAmount);
                    saveAccount(accountId, account);
                });
    }

    private boolean belongsToAccount(JsonObject transaction, Long accountId) {
        return accountId.equals(transaction.getLong(ACCOUNT_ID)) ||
                accountId.equals(transaction.getLong(SENDER_ACCOUNT_ID)) ||
                accountId.equals(transaction.getLong(RECEIVER_ACCOUNT_ID));
    }

    private LocalMap<Object, Object> transactions() {
        return vertx.sharedData().getLocalMap(TRANSACTIONS_MAP);
    }

    private LocalMap<Object, Object> accounts() {
        return vertx.sharedData().getLocalMap(ACCOUNTS_MAP);
    }

    private boolean accountNotExists(Long accountId) {
        return !accounts().containsKey(accountId);
    }

    private JsonObject getAccount(Long accountId) {
        return (JsonObject) accounts().get(accountId);
    }

    private void saveAccount(Long accountId, JsonObject account) {
        accounts().put(accountId, account);
    }

    private Integer effectiveAmount(JsonObject transactionJsonObj, Long accountId) {
        Integer amount = transactionJsonObj.getInteger(AMOUNT);
        String operation = transactionJsonObj.getString(OPERATION);
        Long senderAccountId = transactionJsonObj.getLong(SENDER_ACCOUNT_ID);
        if (Operation.withdraw.name().equals(operation) || accountId.equals(senderAccountId)) {
            amount = -amount;
        }
        return amount;
    }

    private JsonObject toHistoryRow(JsonObject transaction, Long accountId) {
        return new JsonObject()
                .put(ID, transaction.getLong(ID))
                .put(TIMESTAMP, transaction.getLong(TIMESTAMP))
                .put(OPERATION, transaction.getString(OPERATION))
                .put(AMOUNT, effectiveAmount(transaction, accountId));
    }
}
