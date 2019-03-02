import { config as AWSConfig, DynamoDB } from "aws-sdk";
import * as moment from "moment";
import winston = require("winston");

import Reading from "./reading";

export class Uploader {
    private lastTimestamp: number = null;
    private knownExistingTables: {[index: string]: Promise<void>} = {};
    private dynamodb = new DynamoDB();
    private logger = winston.loggers.get("default").child({module: "uploader"});

    public async addReading(reading: Reading) {
        if (this.lastTimestamp === reading.timestamp) {
            this.logger.debug("Repeated reading - skipping");
            return;
        }
        this.lastTimestamp = reading.timestamp;
        this.logger.debug("Uploading %s", reading.timestamp);
        const date = moment.unix(reading.timestamp);
        const tableName = `temp${date.format("YYYYMM")}`;
        if (!this.knownExistingTables[tableName]) {
            this.logger.info("Table %s not yet known", tableName);

            this.knownExistingTables[tableName] = (async () => {
                try {
                    this.logger.debug("Checking if table exists");
                    const describeTableResult = await this.dynamodb.describeTable({ TableName: tableName }).promise();
                    this.logger.info("Table exists");
                } catch (err) {
                    if (err.code === "ResourceNotFoundException") {
                        this.logger.info("Table does not exist - creating");
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
                        this.logger.info("Table created");
                    } else {
                        throw err;
                    }
                }
                this.logger.debug("Waiting until table ready");
                await this.dynamodb.waitFor("tableExists", { TableName: tableName }).promise();
                this.logger.info("Table ready");
            })();
        }

        await this.knownExistingTables[tableName];

        this.logger.debug("Calling putItem");
        const putItemParams: DynamoDB.PutItemInput = {
            Item: {
                hex: { S: reading.hex },
                temp: { N: reading.temp.toFixed(1) },
                timestamp: { N: reading.timestamp.toFixed() },
            },
            TableName: tableName,
        };
        await this.dynamodb.putItem(putItemParams);
        this.logger.info("Upload completed", {reading});
    }
}

export default Uploader;
