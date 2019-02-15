var AWS = require("aws-sdk");
var format = require('date-format');

AWS.config.loadFromPath('aws-secret.json');

var dynamodb = new AWS.DynamoDB();

const readline = require('readline');

const rl = readline.createInterface({
  input: process.stdin
});

var lastSuccessTimestamp = null;

rl.on('line', (line) => {
    var item = parse(line);
    if (item != null)
        console.debug(item.timestamp);
});

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
        console.error("second does not contain only 0 and 1");
        return null;
    }

    if (arr[1].length !== 36)
    {
        console.error("length <> 36");
        return null;
    }
        

    var channel = parseInt(arr[1].substring(10, 12), 2);
    if (channel !== 1)
    {
        console.error("channel !== 1");
        return null;
    }

    var temp = (parseInt(arr[1].substring(12, 24), 2) - 500)/10;

    var completeHex = ("000000000" + parseInt(arr[1], 2).toString(16)).slice(-16);
    return {
        timestamp: timestamp,
        temp: temp,
        hex: completeHex
    };
}



// var params = {
//     TableName : `test_table${format.asString('MMdd', new Date())}`,
//     KeySchema: [       
//         { AttributeName: "timestamp", KeyType: "HASH"},  //Partition key
//     ],
//     AttributeDefinitions: [       
//         { AttributeName: "timestamp", AttributeType: "N" },
//         // { AttributeName: "title", AttributeType: "S" }
//     ],
//     BillingMode: "PAY_PER_REQUEST"
//     // ProvisionedThroughput: {       
//     //     ReadCapacityUnits: 10, 
//     //     WriteCapacityUnits: 10
//     // }
// };

// dynamodb.createTable(params, function(err, data) {
//     if (err) {
//         console.error("Unable to create table. Error JSON:", JSON.stringify(err, null, 2));
//     } else {
//         console.log("Created table. Table description JSON:", JSON.stringify(data, null, 2));
//     }
// });
