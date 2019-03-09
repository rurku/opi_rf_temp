import { config as AWSConfig, DynamoDB } from "aws-sdk";
import * as moment from "moment";
import winston = require("winston");

import Reading from "./reading";

const tableName = "temperature";

export class Uploader {
    private lastTimestamp: number = undefined;
    private timestampInitialized: Promise<void>;

    private dynamodb = new DynamoDB();
    private logger = winston.loggers.get("default").child({module: "uploader"});
    private tablePromise: Promise<void>;

    public async addReading(reading: Reading) {
        await this.ensureTableExists();
        await this.ensureTimestampInitialized(reading.timestamp);

        if (Math.abs(this.lastTimestamp - reading.timestamp) < 2) {
            this.logger.debug("Repeated reading - skipping");
            return;
        }
        const lastTimestamp = this.lastTimestamp;
        this.lastTimestamp = reading.timestamp;

        this.logger.debug("Uploading %s", reading.timestamp);

        const putItemParams: DynamoDB.PutItemInput = {
            Item: {
                DayKey: { S: this.formatDayKey(reading.timestamp) },
                // full resolution readings expire after 1 month
                ExpiresAt: { N: (moment.unix(reading.timestamp).utc().add(1, "months").unix()).toFixed() },
                Hex: { S: reading.hex },
                Temp: { N: reading.temp.toFixed(1) },
                Timestamp: { N: reading.timestamp.toFixed() },

            },
            TableName: tableName,
        };

        if (lastTimestamp === null
            || Math.floor(reading.timestamp / (15 * 60)) !== Math.floor(lastTimestamp  / (15 * 60))) {
            putItemParams.Item.MonthKey = { S: this.formatMonthKey(reading.timestamp) };
            // 15m readings expire after 3 years
            const expiresAt = moment.unix(reading.timestamp).utc().add(3, "years").unix();
            putItemParams.Item.ExpiresAt = { N: expiresAt.toFixed() };
        }

        if (lastTimestamp === null
            || Math.floor(reading.timestamp / (3 * 60 * 60)) !== Math.floor(lastTimestamp / (3 * 60 * 60))) {
            putItemParams.Item.YearKey = { S: this.formatYearKey(reading.timestamp) };
            // 3h readings never expire
            putItemParams.Item.ExpiresAt = undefined;
        }
        this.logger.debug("Calling putItem", {putItemParams});
        await this.dynamodb.putItem(putItemParams).promise();
        this.logger.info("Upload completed", {reading});
    }

    private async ensureTableExists() {
        if (!this.tablePromise) {
            this.logger.info("Checking if table exists", {tableName});

            this.tablePromise = (async () => {
                try {
                    const describeTableResult = await this.dynamodb.describeTable({ TableName: tableName }).promise();
                    this.logger.info("Table exists");
                } catch (err) {
                    if (err.code === "ResourceNotFoundException") {
                        this.logger.info("Table does not exist - creating");
                        const params: DynamoDB.CreateTableInput =  {
                            AttributeDefinitions: [
                                { AttributeName: "Timestamp", AttributeType: "N" },
                                { AttributeName: "DayKey", AttributeType: "S" },
                                { AttributeName: "MonthKey", AttributeType: "S" },
                                { AttributeName: "YearKey", AttributeType: "S" },
                            ],
                            BillingMode: "PAY_PER_REQUEST",
                            GlobalSecondaryIndexes: [
                                {
                                    IndexName: "ByMonth",
                                    KeySchema: [
                                        { AttributeName: "MonthKey", KeyType: "HASH" },
                                        { AttributeName: "Timestamp", KeyType: "RANGE" },
                                    ],
                                    Projection: { ProjectionType: "ALL" },
                                },
                                {
                                    IndexName: "ByYear",
                                    KeySchema: [
                                        { AttributeName: "YearKey", KeyType: "HASH" },
                                        { AttributeName: "Timestamp", KeyType: "RANGE" },
                                    ],
                                    Projection: { ProjectionType: "ALL" },
                                },

                            ],
                            KeySchema: [
                                { AttributeName: "DayKey", KeyType: "HASH" },
                                { AttributeName: "Timestamp", KeyType: "RANGE" },
                            ],
                            TableName: tableName,
                        };
                        await this.dynamodb.createTable(params).promise();
                        this.logger.info("Table created");
                        this.logger.debug("Waiting until table ready");
                        await this.dynamodb.waitFor("tableExists", { TableName: tableName }).promise();
                        this.logger.info("Configuring TTL");
                        await this.dynamodb.updateTimeToLive({
                            TableName: tableName,
                            TimeToLiveSpecification: { AttributeName: "ExpiresAt", Enabled: true },
                        }).promise();
                    } else {
                        throw err;
                    }
                }
                this.logger.debug("Waiting until table ready");
                await this.dynamodb.waitFor("tableExists", { TableName: tableName }).promise();
                this.logger.info("Table ready");
            })();
        }

        await this.tablePromise;
    }

    private formatDayKey(timestamp: number): string {
        return moment.unix(timestamp).utc().format("YYYY-MM-DD");
    }

    private formatMonthKey(timestamp: number): string {
        return moment.unix(timestamp).utc().format("YYYY-MM");
    }

    private formatYearKey(timestamp: number): string {
        return moment.unix(timestamp).utc().format("YYYY");
    }

    private async queryLastTimestamp(currentTimestamp: number): Promise<number> {
        const partitionKey = this.formatDayKey(currentTimestamp) ;
        const query: DynamoDB.QueryInput = { TableName: tableName };
        query.Limit = 1;
        query.KeyConditionExpression = "DayKey = :partitionKey";
        query.ExpressionAttributeValues = {":partitionKey": {S: partitionKey}};
        query.ScanIndexForward = false;
        query.ProjectionExpression = "#ts";
        query.ExpressionAttributeNames = {"#ts": "Timestamp"};
        this.logger.debug("Querying last day timestamp", {query});
        const result = await this.dynamodb.query(query).promise();
        this.logger.debug("Last day timestamp result", {result});
        if (result.Count > 0) {
            return parseInt(result.Items[0].Timestamp.N, 10);
        } else {
            return null;
        }
    }

    private async ensureTimestampInitialized(currentTimestamp: number) {
        if (!this.timestampInitialized) {
            this.timestampInitialized = (async () => {
                this.logger.info("Initializing last timestamp");
                this.lastTimestamp = await this.queryLastTimestamp(currentTimestamp);
                this.logger.info("Initializing last timestamp completed");
            })();
        }
        await this.timestampInitialized;
    }
}

export default Uploader;
