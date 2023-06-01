Backend WebSocket Server
-Utilizes AWS API Gateway with websocket events like $connect, $disconnect, getMessages, getClients, and sendMessages. These are pointed at a lambda function

A $connect WS event will fire when client connects to WS Server. We will populate the Clients table with a connectionID that we get from API GateWay and a username of the client

A $disconnect will delete a client from the Clients table.

The app will have the following custom WS events:

getClients will return a list of currently connected clients the user can chat with.

sendMessages will allow users to send messages to each other.

getMessages will give the conversation history.

Database implementation is in AWS DynamoDB with a clients table to store information about connected clients and a Messages table that keeps the conversation history for a pair of users. 

FrontEnd will be a static page hosted in S3 bucket.

It'll have a couple of views, one login page where the user can provide a username to be saved in local storage of the browser. The chat view will have a list of connected users and a way to chat with them. 