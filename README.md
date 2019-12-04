# Sample money transfers service
This is a simple money-transfer service exposing REST API. 
Service is implemented with Vert.x toolset and uses Vert.x LocalMap as in-memory data storage.

## Build
```
mvn package
```

## Run
```
java -jar target/money-transfers-api-1.0.0-SNAPSHOT.jar -conf src/main/resources/config.json
```
The service will listen on port 8080 but you can configure port in 
```
src/main/resources/config.json
```
## API
#### Create account
Request
```
POST /accounts
{
    "name":"account1"
}
```
Response:
```
{
    "data":<accountId>
    "timestamp":<timestamp>
}
```
#### Get account
Request
```
GET /accounts/:id
```
Response:
```
{
    "data": {
        "name":"account1",
        "id":<accountId>
        "balance":100
    }
    "timestamp":<timestamp>
}
```
#### Balance operations
   Request
   ```
   POST /accounts/:id/balance
   {
       "operation":"deposit",
       "amount":100
   }
   
   POST /accounts/:id/balance
   {
       "operation":"withdraw",
       "amount":100
   }
   ```
   Response:
   ```
   {
       "data":<transactionId>
       "timestamp":<timestamp>
   }
   ```
#### Get balance
Request
   ```
   GET /accounts/:id/balance
   ```
   Response:
   ```
   {
       "data":500
       "timestamp":<timestamp>
   }
   ```
#### Transfer
Request
```
POST /transfers
{
    "senderAccountId":<accountId>,
    "receiverAccountId":<accountId>,
    "amount":100
}
```
Response:
```
{
    "data":<transactionId>
    "timestamp":<timestamp>
}
```
#### List transactions
Request
```
GET /accounts/:id/transactions
```
Response:
```
{
  "data": [
    {
      "id": 2,
      "timestamp":<timestamp>,
      "operation": "transfer",
      "amount": -500
    },
    {
      "id": 1,
      "timestamp":<timestamp>,
      "operation": "deposit",
      "amount": 1000
    }
  ],
  "timestamp":<timestamp>
}
```
#### Error
Response:
```
{
    "error":"Failure on <path>, reason: <error message>"
}
```