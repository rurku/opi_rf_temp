import { config as AWSConfig, DynamoDB } from "aws-sdk";
import * as moment from "moment";

import Reading from "./reading";

export class Uploader {
    private lastTimestamp: number = null;
    private knownExistingTables: {[index: string]: Promise<void>} = {};
    private dynamodb = new DynamoDB();

    public async addReading(reading: Reading) {
        if (this.lastTimestamp === reading.timestamp) {
            console.debug("Repeated reading - skipping");
            return;
        }
        this.lastTimestamp = reading.timestamp;
        console.debug(`Uploading ${reading.timestamp}`);
        const date = moment.unix(reading.timestamp);
        const tableName = `temp${date.format("YYYYMM")}`;
        if (!this.knownExistingTables[tableName]) {
            console.debug(`Table ${tableName} not yet known`);

            this.knownExistingTables[tableName] = (async () => {
                try {
                    console.debug("Checking if table exists");
                    const describeTableResult = await this.dynamodb.describeTable({ TableName: tableName }).promise();
                    console.debug("Table exists");
                } catch (err) {
                    if (err.code === "ResourceNotFoundException") {
                        console.debug("Table does not exist - creating");
                        const params: DynamoDB.CreateTableInput =  {
                            AttributeDefinitions: [
                                { AttributeName: "timestamp", AttributeType: "N" },
                            ],
                            BillingMode: "PAY_PER_REQUEST",
                            KeySchema: [
                                { AttributeName: "timestamp", KeyType: "HASH" },
                            ],
                            TableName: tableName,
                        };
                        await this.dynamodb.createTable(params).promise();
                        console.debug("Table created");
                    } else {
                        throw err;
                    }
                }
                console.debug("Waiting until table ready");
                await this.dynamodb.waitFor("tableExists", { TableName: tableName }).promise();
                console.debug("Table ready");
            })();
        }

        await this.knownExistingTables[tableName];

        console.debug("Table exists - calling putItem");
        const putItemParams: DynamoDB.PutItemInput = {
            Item: {
                hex: { S: reading.hex },
                temp: { N: reading.temp.toFixed(1) },
                timestamp: { N: reading.timestamp.toFixed() },
            },
            TableName: tableName,
        };
        await this.dynamodb.putItem(putItemParams);
        console.debug(`Upload completed - ${reading.timestamp}`);
    }
}

export default Uploader;
