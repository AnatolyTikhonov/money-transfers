package com.transfers.api;

import io.reactivex.Single;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.ServerSocket;

@RunWith(VertxUnitRunner.class)
public class ApiTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    private Vertx vertx;
    private Integer port;
    private WebClient client;

    @Before
    public void setUp(TestContext context) throws IOException {
        vertx = rule.vertx();
        ServerSocket socket = new ServerSocket(0);
        client = WebClient.create(vertx);
        port = socket.getLocalPort();
        socket.close();
        DeploymentOptions options = new DeploymentOptions()
                .setConfig(new JsonObject().put("http.port", port));
        vertx.deployVerticle(MainVerticle.class.getName(), options, context.asyncAssertSuccess());
    }

    @After
    public void tearDown(TestContext context) {
        vertx.close(context.asyncAssertSuccess());
    }

    @Test
    public void testCreateAccountsSunny(TestContext context) {
        final Async async = context.async();
        createAccount()
                .doOnSuccess(account1Response -> {
                    JsonObject account1ResponseBody = account1Response.bodyAsJsonObject();
                    context.assertEquals(201, account1Response.statusCode());
                    context.assertEquals(1, account1ResponseBody.getInteger("data"));
                    context.assertNotNull(account1ResponseBody.getLong("timestamp"));
                })
                .flatMap(first -> createAccount())
                .subscribe(account2Response -> {
                    JsonObject account2ResponseBody = account2Response.bodyAsJsonObject();
                    context.assertEquals(201, account2Response.statusCode());
                    context.assertEquals(2, account2ResponseBody.getInteger("data"));
                    async.complete();
                });
    }

    @Test
    public void testCreateAccountValidationError1(TestContext context) {
        final Async async = context.async();
        JsonObject account = new JsonObject()
                .put("namee", "acc1");
        createAccount(account)
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(400, response.statusCode());
                    context.assertEquals("Failure on /accounts, reason: $.name: is missing but it is required", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testCreateAccountValidationError2(TestContext context) {
        final Async async = context.async();
        JsonObject account = new JsonObject()
                .put("name", "");
        createAccount(account)
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(400, response.statusCode());
                    context.assertEquals("Failure on /accounts, reason: $.name: must be at least 2 characters long", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testDepositSunny(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId -> deposit(accountId, 1000))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(200, response.statusCode());
                    context.assertEquals(1, responseBody.getInteger("data"));
                    async.complete();
                });
    }

    @Test
    public void testDepositValidationOperationRequired(TestContext context) {
        final Async async = context.async();
        JsonObject depositOperation = new JsonObject()
                .put("operationn", "deposit")
                .put("amount", 1000);
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId -> balanceOperation(accountId, depositOperation))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(400, response.statusCode());
                    context.assertEquals("Failure on /accounts/1/balance, reason: $.operation: is missing but it is required", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testDepositValidationOperationNameIncorrect(TestContext context) {
        final Async async = context.async();
        JsonObject depositOperation = new JsonObject()
                .put("operation", "depositt")
                .put("amount", 1000);
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId -> balanceOperation(accountId, depositOperation))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(400, response.statusCode());
                    context.assertEquals("Failure on /accounts/1/balance, reason: Operation must be or 'deposit' or 'withdraw'", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testDepositValidationAmountRequired(TestContext context) {
        final Async async = context.async();
        JsonObject depositOperation = new JsonObject()
                .put("operation", "deposit")
                .put("amountt", 1000);
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId -> balanceOperation(accountId, depositOperation))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(400, response.statusCode());
                    context.assertEquals("Failure on /accounts/1/balance, reason: $.amount: is missing but it is required", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testDepositValidationAmountPositive(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId -> deposit(accountId, -1000))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(400, response.statusCode());
                    context.assertEquals("Failure on /accounts/1/balance, reason: $.amount: must have a minimum value of 1.0", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testDepositValidationAccountNotFound(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId -> deposit(2L, 1000))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(404, response.statusCode());
                    context.assertEquals("Failure on /accounts/2/balance, reason: Account not found", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testWithdrawSunny(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId -> deposit(accountId, 1000)
                        .map(result -> accountId))
                .flatMap(accountId -> withdraw(accountId, 500))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(200, response.statusCode());
                    context.assertEquals(2, responseBody.getInteger("data"));
                    async.complete();
                });
    }

    @Test
    public void testWithdrawNotEnoughFunds(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId -> deposit(accountId, 1000)
                        .map(result -> accountId))
                .flatMap(accountId -> withdraw(accountId, 1500))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(422, response.statusCode());
                    context.assertEquals("Failure on /accounts/1/balance, reason: Not enough funds", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testTransferSunny(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId1 -> createAccount()
                        .map(this::getEntityId)
                        .flatMap(accountId2 -> deposit(accountId1, 1000)
                                .flatMap(resp -> transfer(accountId1, accountId2, 500))))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(200, response.statusCode());
                    context.assertNotNull(responseBody.getLong("timestamp"));
                    async.complete();
                });
    }

    @Test
    public void testTransferValidationSenderInvalid1(TestContext context) {
        final Async async = context.async();
        JsonObject transfer = new JsonObject()
                .put("senderAccountIdd", 1)
                .put("receiverAccountId", 2)
                .put("amount", 500);
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId1 -> createAccount()
                        .map(this::getEntityId)
                        .flatMap(accountId2 -> deposit(accountId1, 1000)
                                .flatMap(resp -> transfer(transfer))))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(400, response.statusCode());
                    context.assertEquals("Failure on /transfers, reason: $.senderAccountId: is missing but it is required", response.bodyAsJsonObject().getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testTransferValidationSenderInvalid2(TestContext context) {
        final Async async = context.async();
        JsonObject transfer = new JsonObject()
                .put("senderAccountId", -5)
                .put("receiverAccountId", 2)
                .put("amount", 500);
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId1 -> createAccount()
                        .map(this::getEntityId)
                        .flatMap(accountId2 -> deposit(accountId1, 1000)
                                .flatMap(resp -> transfer(transfer))))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(400, response.statusCode());
                    context.assertEquals("Failure on /transfers, reason: $.senderAccountId: must have a minimum value of 1.0", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testTransferValidationReceiverInvalid(TestContext context) {
        final Async async = context.async();
        JsonObject transfer = new JsonObject()
                .put("senderAccountId", 1)
                .put("receiverAccountIdd", 2)
                .put("amount", 500);
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId1 -> createAccount()
                        .map(this::getEntityId)
                        .flatMap(accountId2 -> deposit(accountId1, 1000)
                                .flatMap(resp -> transfer(transfer))))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(400, response.statusCode());
                    context.assertEquals("Failure on /transfers, reason: $.receiverAccountId: is missing but it is required", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testTransferSameAccounts(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId1 -> createAccount()
                        .map(this::getEntityId)
                        .flatMap(accountId2 -> deposit(accountId1, 1000)
                                .flatMap(resp -> transfer(accountId1, accountId1, 500))))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(400, response.statusCode());
                    context.assertEquals("Failure on /transfers, reason: Sender and receiver accounts must be different", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testTransferAccountNotFound(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId1 -> createAccount()
                        .map(this::getEntityId)
                        .flatMap(accountId2 -> deposit(accountId1, 1000)
                                .flatMap(resp -> transfer(accountId1, 3L, 500))))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(404, response.statusCode());
                    context.assertEquals("Failure on /transfers, reason: Account not found", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testTransferNotEnoughFunds(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId1 -> createAccount()
                        .map(this::getEntityId)
                        .flatMap(accountId2 -> deposit(accountId1, 1000)
                                .flatMap(resp -> transfer(accountId1, accountId2, 1500))))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(422, response.statusCode());
                    context.assertEquals("Failure on /transfers, reason: Not enough funds", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testGetAccountSunny(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(this::getAccount)
                .subscribe(response -> {
                    JsonObject accountJsonObj = response.bodyAsJsonObject().getJsonObject("data");
                    context.assertEquals(200, response.statusCode());
                    context.assertEquals(1, accountJsonObj.getInteger("id"));
                    context.assertEquals(0, accountJsonObj.getInteger("balance"));
                    async.complete();
                });
    }

    @Test
    public void testGetAccountNotFound(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId -> getAccount(2L))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(404, response.statusCode());
                    context.assertEquals("Failure on /accounts/2, reason: Account not found", responseBody.getString("error"));
                    async.complete();
                });
    }

    @Test
    public void testGetAccountWithBalanceAfterDeposit(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId -> deposit(accountId, 1000)
                        .flatMap(response -> getAccount(accountId)))
                .subscribe(response -> {
                    JsonObject accountJsonObj = response.bodyAsJsonObject().getJsonObject("data");
                    context.assertEquals(200, response.statusCode());
                    context.assertEquals(1000, accountJsonObj.getInteger("balance"));
                    async.complete();
                });
    }

    @Test
    public void testGetAccountWithBalanceAfterDepositWithdraw(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId -> deposit(accountId, 1000)
                        .flatMap(response -> withdraw(accountId, 500))
                        .flatMap(response -> getAccount(accountId)))
                .subscribe(response -> {
                    JsonObject accountJsonObj = response.bodyAsJsonObject().getJsonObject("data");
                    context.assertEquals(200, response.statusCode());
                    context.assertEquals(500, accountJsonObj.getInteger("balance"));
                    async.complete();
                });
    }

    @Test
    public void testGetAccountWithBalanceAfterTransfer(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId1 -> createAccount()
                        .map(this::getEntityId)
                        .flatMap(accountId2 -> deposit(accountId1, 1000)
                                .flatMap(resp -> transfer(accountId1, accountId2, 500))
                        .flatMap(response -> getAccount(accountId2))))
                .subscribe(response -> {
                    JsonObject accountJsonObj = response.bodyAsJsonObject().getJsonObject("data");
                    context.assertEquals(200, response.statusCode());
                    context.assertEquals(500, accountJsonObj.getInteger("balance"));
                    async.complete();
                });
    }

    @Test
    public void testGetTransactionsSunny(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId1 -> createAccount()
                        .map(this::getEntityId)
                        .flatMap(accountId2 -> deposit(accountId1, 1000)
                                .flatMap(resp -> transfer(accountId1, accountId2, 500)
                                .flatMap(response -> transactions(accountId1)))))
                .subscribe(response -> {
                    JsonArray transactionsJsonArr = response.bodyAsJsonObject().getJsonArray("data");
                    context.assertEquals(200, response.statusCode());
                    context.assertEquals("transfer", transactionsJsonArr.getJsonObject(0).getString("operation"));
                    context.assertEquals(-500, transactionsJsonArr.getJsonObject(0).getInteger("amount"));
                    context.assertEquals("deposit", transactionsJsonArr.getJsonObject(1).getString("operation"));
                    context.assertEquals(1000, transactionsJsonArr.getJsonObject(1).getInteger("amount"));
                    async.complete();
                });
    }

    @Test
    public void testGetTransactionsAccountNotFound(TestContext context) {
        final Async async = context.async();
        createAccount()
                .map(this::getEntityId)
                .flatMap(accountId1 -> createAccount()
                        .map(this::getEntityId)
                        .flatMap(accountId2 -> deposit(accountId1, 1000)
                                .flatMap(resp -> transfer(accountId1, accountId2, 500)
                                        .flatMap(response -> transactions(3L)))))
                .subscribe(response -> {
                    JsonObject responseBody = response.bodyAsJsonObject();
                    context.assertEquals(404, response.statusCode());
                    context.assertEquals("Failure on /accounts/3/transactions, reason: Account not found", responseBody.getString("error"));
                    async.complete();
                });
    }

    private Single<HttpResponse<Buffer>> createAccount() {
        JsonObject account = new JsonObject()
                .put("name", "acc1");
        return createAccount(account);
    }

    private Single<HttpResponse<Buffer>> createAccount(JsonObject account) {
        return Single.create(emitter -> client.post(port, "localhost", "/accounts")
                .sendJsonObject(account, asyncResponse -> emitter.onSuccess(asyncResponse.result())));
    }

    private Single<HttpResponse<Buffer>> getAccount(Long accountId) {
        return Single.create(emitter -> client.get(port, "localhost", String.format("/accounts/%s", accountId))
                .send(asyncResponse -> emitter.onSuccess(asyncResponse.result())));
    }

    private Single<HttpResponse<Buffer>> deposit(Long accountId, Integer amount) {
        JsonObject depositOperation = new JsonObject()
                .put("operation", "deposit")
                .put("amount", amount);
        return balanceOperation(accountId, depositOperation);
    }

    private Single<HttpResponse<Buffer>> withdraw(Long accountId, Integer amount) {
        JsonObject withdrawOperation = new JsonObject()
                .put("operation", "withdraw")
                .put("amount", amount);
        return balanceOperation(accountId, withdrawOperation);
    }

    private Single<HttpResponse<Buffer>> balanceOperation(Long accountId, JsonObject balanceOperation) {
        return Single.create(emitter -> client.post(port, "localhost", String.format("/accounts/%s/balance", accountId))
                .sendJsonObject(balanceOperation, asyncResponse -> emitter.onSuccess(asyncResponse.result())));
    }

    private Single<HttpResponse<Buffer>> transfer(Long senderAccountId, Long receiverAccountId, Integer amount) {
        JsonObject transfer = new JsonObject()
                .put("senderAccountId", senderAccountId)
                .put("receiverAccountId", receiverAccountId)
                .put("amount", amount);
        return transfer(transfer);
    }

    private Single<HttpResponse<Buffer>> transfer(JsonObject transfer) {
        return Single.create(emitter -> client.post(port, "localhost", "/transfers")
                .sendJsonObject(transfer, asyncResponse -> emitter.onSuccess(asyncResponse.result())));
    }

    private Single<HttpResponse<Buffer>> transactions(Long accountId) {
        return Single.create(emitter -> client.get(port, "localhost", String.format("/accounts/%s/transactions", accountId))
                .send(asyncResponse -> emitter.onSuccess(asyncResponse.result())));
    }

    private Long getEntityId(HttpResponse<Buffer> response) {
        return response.bodyAsJsonObject().getLong("data");
    }

}
