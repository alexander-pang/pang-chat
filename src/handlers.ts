import { APIGatewayProxyEvent, APIGatewayProxyEventQueryStringParameters, APIGatewayProxyResult } from "aws-lambda";
import AWS, { AWSError } from "aws-sdk";

//Types
type Action = "getClients" | "sendMessage" | "getMessages" | "$disconnect" | "$connect";
type Client = {
  connectionId: string;
  nickname: string;
};

//Globals
const CLIENT_TABLE_NAME = "Clients";
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

  switch (routeKey) {
    case "getClients":
      handleGetClients(connectionId);
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
        ":nickname": queryStringParameters["nickname"],
      },
    })
    .promise();

  // case 1: client connects to chat and doesn't disconnect properly and tries to connect again
  if (output.Count && output.Count > 0) {
    const client = (output.Items as Client[])[0];
    if (await postToConnection(client.connectionId, JSON.stringify({ type: "ping" }))) {
      return forbidden;
    }
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

const buildClientsMessage = (clients: Client[]): string => JSON.stringify({ type: "clients", value: { clients } });

