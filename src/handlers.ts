import { APIGatewayProxyEvent, APIGatewayProxyEventQueryStringParameters, APIGatewayProxyResult } from "aws-lambda";
import AWS, { AWSError } from "aws-sdk";
import { Key } from "aws-sdk/clients/cloudformation";
import { StringReference } from "aws-sdk/clients/connect";
import { v4 } from "uuid";

//Types
type Action = "getClients" | "sendMessage" | "getMessages" | "$disconnect" | "$connect";
type Client = {
  connectionId: string;
  nickname: string;
};
type GetMessagesBody = {
  targetNickname: string;
  limit: number;
  smartKey: Key | undefined;
}
type SendMessageBody = {
  message: string;
  receiverNickname: string;
}

//Globals
class HandleError extends Error {};
const CLIENT_TABLE_NAME = "Clients";
const MESSAGES_TABLE_NAME = "Messages"
const twoHundredResponse = {
  statusCode: 200,
  body: "",
};
const forbidden = {
  statusCode: 403,
  body: "",
};

const docClient = new AWS.DynamoDB.DocumentClient();
const apiGWManagementAPI = new AWS.ApiGatewayManagementApi({
  endpoint: process.env["WSSAPIGATEWAYENDPOINT"],
});

//client connects to websocket server
export const handle = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  // wss://amazonaws.com?nickname=alex
  const connectionId = event.requestContext.connectionId as string;
  const routeKey = event.requestContext.routeKey as Action;

  try{
    switch (routeKey) {
      case "getMessages":
        return handleGetMessages(connectionId, parseGetMessagesBody(event.body));
      case "sendMessage":
        return handleSendMessage(connectionId, parseSendMessageBody(event.body));
      case "getClients":
        return handleGetClients(connectionId);
      case "$disconnect":
        return handleDisconnect(connectionId);
      case "$connect":
        return handleConnect(connectionId, event.queryStringParameters);
      default:
        return {
          statusCode: 500,
          body: "",
        };
      }
  } catch(e){
    if (e instanceof HandleError){
      await postToConnection(connectionId, e.message);
      return twoHundredResponse;
    }
    throw e;
  }

  
  };

const getEveryClient = async (): Promise<Client[]> => {
  const output = await docClient
    .scan({
      TableName: CLIENT_TABLE_NAME,
    })
    .promise();
  const clients = output.Items || [];
  return clients as Client[];
};

const postToConnection = async (connectionId: string, data: string): Promise<boolean> => {
  try {
    await apiGWManagementAPI
      .postToConnection({
        ConnectionId: connectionId,
        Data: JSON.stringify(data),
      })
      .promise();
    return true;
  } catch (e) {
    //handle a stale connection
    if ((e as AWSError).statusCode !== 410) {
      throw e;
    }
    await docClient
      .delete({
        TableName: CLIENT_TABLE_NAME,
        Key: {
          connectionId,
        },
      })
      .promise();
    return false;
  }
};

const notifyClients = async (connectionIdToIgnore: string) => {
  const clients = await getEveryClient();

  await Promise.all(
    clients
      .filter((client) => client.connectionId !== connectionIdToIgnore)
      .map(async (client) => {
        await postToConnection(client.connectionId, buildClientsMessage(clients));
      }),
  );
};

const handleGetMessages =async (
  connectionId:string,
  body: GetMessagesBody,
  ): Promise<APIGatewayProxyResult> => {
    body.targetNickname
  const queryOutput = docClient.query({
    TableName: MESSAGES_TABLE_NAME,
    IndexName: "NicknameToNicknameIndex",
          //name may be restricted by AWS so placeholder
          KeyConditionExpression: "#nicknameToNickname = :nicknameToNickname",
          ExpressionAttributeNames: {
            "#nicknameToNickname": "nicknameToNickname",
          },
          ExpressionAttributeValues: {
            ":nicknameToNickname": nickname,
          },

  })
}

const handleGetClients = async (connectionId: string): Promise<APIGatewayProxyResult> => {
  const output = await docClient
    .scan({
      TableName: CLIENT_TABLE_NAME,
    })
    .promise();

  //if no clients then we'll have an empty list
  const clients = await getEveryClient();

  await postToConnection(connectionId, buildClientsMessage(clients));

  return twoHundredResponse;
};

const handleDisconnect = async (connectionId: string): Promise<APIGatewayProxyResult> => {
  await docClient
    .delete({
      TableName: CLIENT_TABLE_NAME,
      Key: {
        connectionId,
      },
    })
    .promise();
  await notifyClients(connectionId);
  return twoHundredResponse;
};

const handleConnect = async (
  connectionId: string,
  queryStringParameters: APIGatewayProxyEventQueryStringParameters | null,
): Promise<APIGatewayProxyResult> => {
  if (!queryStringParameters || !queryStringParameters["nickname"]) {
    return forbidden;
  }
  // case 1: client connects to chat and doesn't disconnect properly and tries to connect again

  const existingConnectionId = await getConnectioinIdByNickname(queryStringParameters["nickname"]);
  if (existingConnectionId && JSON.stringify({ type: "ping" })) {
      return forbidden;
  }

  await docClient
    .put({
      TableName: CLIENT_TABLE_NAME,
      Item: {
        connectionId,
        nickname: queryStringParameters["nickname"],
      },
    })
    .promise();

  await notifyClients(connectionId);

  return twoHundredResponse;
};

const getConnectioinIdByNickname = async (nickname:string): Promise<string | undefined> => {
  const output = await docClient
    .query({
      TableName: CLIENT_TABLE_NAME,
      IndexName: "NicknameIndex",
      //name may be restricted by AWS so placeholder
      KeyConditionExpression: "#nickname = :nickname",
      ExpressionAttributeNames: {
        "#nickname": "nickname",
      },
      ExpressionAttributeValues: {
        ":nickname": nickname,
      },
    })
    .promise();

  // case 1: client connects to chat and doesn't disconnect properly and tries to connect again
  if (output.Count && output.Count > 0) {
    const client = (output.Items as Client[])[0];
    return client.connectionId;
  }
  return undefined;
}

const buildClientsMessage = (clients: Client[]): string => JSON.stringify({ type: "clients", value: { clients } });

const handleSendMessage = async (
  senderConnectionId:string,
  body:SendMessageBody
  ): Promise<APIGatewayProxyResult> => {
  //1. create a message in messages table
  
  const outputFromClientTable = await docClient.get({
    TableName: CLIENT_TABLE_NAME,
    Key: {
      connectionId: senderConnectionId,
    },
  })
  .promise();

  const sender = outputFromClientTable.Item as Client;

  // ensures that conversations between senderreceiver aren't also stored as receiversender  
  const nicknameToNickname = [sender.nickname, body.receiverNickname].sort().join("#");

  await docClient.put({
    TableName: MESSAGES_TABLE_NAME,
    Item: {
      messageId: v4(),
      createdAt: new Date().getMilliseconds(),
      nicknameToNickname: nicknameToNickname,
      message: body.message,
      sender: sender.nickname,
    },
  })
  .promise();

  //2. send message to receiver connection id if connected

  const receiverConnectionId = await getConnectioinIdByNickname(body.receiverNickname);

  if (receiverConnectionId){
    await postToConnection(receiverConnectionId, JSON.stringify({
      type: "message",
      value: {
        sender: sender.nickname,
        message: body.message,
      },
    }))
  }
  return twoHundredResponse;
};

const parseSendMessageBody = (body:string | null): SendMessageBody => {
  const SendMessageBody = JSON.parse(body || "{}") as SendMessageBody;

  if(!SendMessageBody || 
    typeof SendMessageBody.message !== "string" 
    || typeof SendMessageBody.receiverNickname !== "string"){
      throw new HandleError("incorrect SendMessageBody format");
    }

  return SendMessageBody;
}

const parseGetMessagesBody = (body:string | null): GetMessagesBody => {
  const getMessagesBody = JSON.parse(body || "{}") as GetMessagesBody

  if(!getMessagesBody ||
    typeof getMessagesBody.targetNickname !== "string" ||
    typeof getMessagesBody.limit !== "number"
  ){
    throw new HandleError("incorrect GetNessages format");
  }
  return getMessagesBody;
}

