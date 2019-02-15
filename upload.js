var AWS = require("aws-sdk");
var format = require('date-format');

AWS.config.loadFromPath('aws-secret.json');

var dynamodb = new AWS.DynamoDB();

const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin
});

var lastSuccessTimestamp = null;
var knownExistingTables = [];

rl.on('line', (line) => {
    var item = parse(line);
    if (item != null)
    {   
        if (lastSuccessTimestamp === null || Math.abs(lastSuccessTimestamp - item.timestamp) >= 2)
        {
            lastSuccessTimestamp = item.timestamp;
            upload(item).then(undefined, error => console.error(`Error uploading item ${item.timestamp} - ${JSON.stringify(error)}`));
        }
    }
});

async function asyncWrapper(fn, params) {
    return new Promise((resolve, reject) => {
        fn(params, (err, data) => {
            if (err) {
                reject(err);
            } else {
                resolve(data);
            }
        });
    });
}

async function upload(item)
{
    console.debug(`Uploading ${item.timestamp}`);
    var date = new Date(item.timestamp*1000);
    var tableName = `temp${format.asString('yyyyMM', date)}`
    if (!knownExistingTables[tableName])
    {
        console.debug(`Table ${tableName} not yet known`);
        
        knownExistingTables[tableName] = (async () => {

            try {
                console.debug("Checking if table exists");
                await asyncWrapper((p,c) => dynamodb.describeTable(p,c), { TableName: tableName });
                console.debug("Table exists")
            }
            catch (err)
            {
                if (err.code === "ResourceNotFoundException")
                {
                    console.debug("Table does not exist - creating");
                    var params = {
                        TableName : tableName,
                        KeySchema: [       
                            { AttributeName: "timestamp", KeyType: "HASH"},  //Partition key
                        ],
                        AttributeDefinitions: [       
                            { AttributeName: "timestamp", AttributeType: "N" },
                        ],
                        BillingMode: "PAY_PER_REQUEST"
                    };
                    await asyncWrapper((p,c) => dynamodb.createTable(p,c), params);
                    console.debug("Table created");
                }
                else
                {
                    throw err;
                }
            }
            console.debug("Waiting until table ready");
            await asyncWrapper((p,c) => dynamodb.waitFor("tableExists", p, c), { TableName: tableName });
            console.debug("Table ready");
        })();
    }

    await knownExistingTables[tableName];

    console.debug("Table exists - calling putItem");
    var putItemParams = {
        Item: {
            "timestamp": {
                N: item.timestamp.toFixed()
            },
            "temp": {
                N: item.temp.toFixed(1)
            },
            "hex": {
                S: item.hex
            }
        },
        TableName: tableName
    };
    await asyncWrapper((p,c) => dynamodb.putItem(p,c), putItemParams);
    console.debug(`Upload completed - ${item.timestamp}`);
}

function parse(line)
{
    var arr = line.split(" ");
    if (arr.length !== 2)
    {
        console.error("line does not contain 2 fields");
        return null;
    }

    var timestamp = parseInt(arr[0])
    if (!Number.isInteger(timestamp))
    {
        console.error(`first field is not an integer: ${arr[0]}`);
        return null;
    }

    if (!arr[1].match(/[01]+/))
    {
        console.error("second field does not contain only 0 and 1");
        return null;
    }

    if (arr[1].length !== 36)
    {
        console.debug("length <> 36");
        return null;
    }
        

    var channel = parseInt(arr[1].substring(10, 12), 2);
    if (channel !== 1)
    {
        console.debug("channel !== 1");
        return null;
    }

    var temp = (parseInt(arr[1].substring(12, 24), 2) - 500)/10;

    var completeHex = ("000000000" + parseInt(arr[1], 2).toString(16)).slice(-9);
    return {
        timestamp: timestamp,
        temp: temp,
        hex: completeHex
    };
}
