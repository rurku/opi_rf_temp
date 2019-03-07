import * as AWS from "aws-sdk";
import * as commander from "commander";
import * as fs from "fs";
import * as readline from "readline";
import * as winston from "winston";

import Analyzer from "./analyzer";
import IMessage from "./message";
import Reading from "./reading";
import Uploader from "./uploader";

commander
    .option("-i, --input-file <file>", "Input file")
    .option("-c, --channel <channel>", "Channel 1, 2 or 3")
    .option("--log-level <level>", "Log level")
    .parse(process.argv);

let inputStream: NodeJS.ReadableStream;
let channel: number;
let logLevel: string = "info";

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

if (commander.logLevel) {
    logLevel = commander.logLevel;
}

// this can be changed to winston's default logger once this is released
// https://github.com/winstonjs/winston/pull/1603
// Then all instances of winston.loggers.get("default").child() should be changed to simply winston.child()
winston.loggers.add("default", {
    format: winston.format.combine(winston.format.splat(), winston.format.simple()),
    level: logLevel,
    transports: [ new winston.transports.Console() ],
});

const logger = winston.loggers.get("default").child({module: "upload"});

logger.debug("loading AWS config from %s", "aws-secret.json");
AWS.config.loadFromPath("aws-secret.json");

async function processInput(stream: NodeJS.ReadableStream) {
    const uploader = new Uploader();
    const analyzer = new Analyzer(async (m: IMessage) => {
        logger.debug("Extracted message", { m });
        const reading = decode(m);
        logger.debug("Decodecd reading", { reading });
        if (reading != null && reading.channel === channel) {
            await uploader.addReading(reading);
        }
    });

    const rl = readline.createInterface(stream);

    for await (const line of rl) {
        logger.silly("analyzing line: %s", line);
        analyzer.addLine(line);
    }
    logger.debug("Finished reading input stream");
}

function decode(message: IMessage): Reading {
    if (message.payload.length !== 36) {
        logger.debug("Ignoring message because length !== 36", {m: message});
        return null;
    }
    if (!verifyChecksum(message.payload)) {
        logger.info("Checksum error", {m: message});
        return null;
    }
    const ret = new Reading();
    ret.channel = parseInt(message.payload.substring(10, 12), 2);
    ret.temp = (parseInt(message.payload.substring(12, 24), 2) - 500) / 10;
    ret.hex = ("000000000" + parseInt(message.payload, 2).toString(16)).slice(-9);
    ret.timestamp = message.timestamp;
    return ret;
}

function verifyChecksum(payload: string): boolean {
    const bytes: number[] = [];
    for (let i = 0; i < 32; i += 8) { // the message length is 36 but last 4 bits are not used for checksum
        const nibble1 = parseInt(payload.substr(i, 4), 2);
        const nibble2 = parseInt(payload.substr(i + 4, 4), 2);
        // assemble the nibbles of each byte in reverse order
        bytes.push(nibble2 * 16 + nibble1);
    }

    const seed = 0x66;
    const checksum = (seed + bytes[0] + bytes[1] + bytes[2]) % 256;
    return checksum === bytes[3];
}

processInput(inputStream);
