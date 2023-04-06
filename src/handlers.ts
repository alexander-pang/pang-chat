import { APIGatewayProxyEvent, APIGatewayProxyEventQueryStringParameters, APIGatewayProxyResult } from "aws-lambda";
import AWS, { AWSError } from "aws-sdk";

//Types
type Action = "getClients" | "sendMessage" | "getMessages" | "$disconnect" | "$connect";

//Globals
const CLIENT_TABLE_NAME = "Clients";
const twoHundredResponse = {
  statusCode: 200,
  body: "",
};

const docClient = new AWS.DynamoDB.DocumentClient();
const apiGWManagementAPI = new AWS.ApiGatewayManagementApi({
  endpoint: process.env["WSSAPIGATEWAYENDPOINT"],
});

export const handle = async (event: APIGatewayProxyEvent): Promise<APIGatewayProxyResult> => {
  // wss://asds.aws.com?nickname=alex
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

const notifyClients = async (connectionIdToIgnore: string) : Promise<APIGatewayProxyResult> => {
  
}

const handleGetClients = async (connectionId: string): Promise<APIGatewayProxyResult> => {
  const output = await docClient
    .scan({
      TableName: CLIENT_TABLE_NAME,
    })
    .promise();

  //if no clients then we'll have an empty list
  const clients = output.Items || [];

  try {
    await apiGWManagementAPI
      .postToConnection({
        ConnectionId: connectionId,
        Data: JSON.stringify(clients),
      })
      .promise();
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
  }

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

  return twoHundredResponse;
};

const handleConnect = async (
  connectionId: string,
  queryStringParameters: APIGatewayProxyEventQueryStringParameters | null,
): Promise<APIGatewayProxyResult> => {
  if (!queryStringParameters || !queryStringParameters["nickname"]) {
    return {
      statusCode: 403,
      body: "",
    };
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

  return twoHundredResponse;
};
