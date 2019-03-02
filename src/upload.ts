import * as AWS from "aws-sdk";
import * as commander from "commander";
import * as fs from "fs";
import * as readline from "readline";

import Analyzer from "./analyzer";
import IMessage from "./message";
import Reading from "./reading";
import Uploader from "./uploader";

commander
    .option("-i, --input-file <file>", "Input file")
    .option("-c, --channel <channel>", "Channel 1, 2 or 3")
    .parse(process.argv);

let inputStream: NodeJS.ReadableStream;
let channel: number;

if (commander.inputFile) {
    inputStream = fs.createReadStream(commander.inputFile);
} else {
    inputStream = process.stdin;
}

if (commander.channel) {
    channel = parseInt(commander.channel, 10);
    if (![1, 2, 3].includes(channel)) {
        throw new Error("Available channels are 1, 2 and 3");
    }
} else {
    channel = 1;
}

AWS.config.loadFromPath("aws-secret.json");

async function processInput(stream: NodeJS.ReadableStream) {
    const uploader = new Uploader();
    const analyzer = new Analyzer(async (m: IMessage) => {
        const reading = decode(m);
        if (reading != null && reading.channel === channel) {
            await uploader.addReading(reading);
        }
    });

    const rl = readline.createInterface(stream);

    for await (const line of rl) {
        analyzer.addLine(line);
    }
}

function decode(message: IMessage): Reading {
    if (message.payload.length !== 36) {
        process.stderr.write(`Ignoring message because length !== 36: ${JSON.stringify(message)}\n`);
    }
    const ret = new Reading();
    ret.channel = parseInt(message.payload.substring(10, 12), 2);
    ret.temp = (parseInt(message.payload.substring(12, 24), 2) - 500) / 10;
    ret.hex = ("000000000" + parseInt(message.payload, 2).toString(16)).slice(-9);
    ret.timestamp = message.timestamp;
    return ret;
}

processInput(inputStream);
