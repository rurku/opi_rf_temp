import commander = require("commander");
import fs = require("fs");
import readline = require("readline");

import Analyzer from "./analyzer";
import IMessage from "./message";
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

async function processInput(stream: NodeJS.ReadableStream) {
    const uploader = new Uploader();
    const analyzer = new Analyzer((m: IMessage) => {
        uploader.addMessage(m);
    });

    const rl = readline.createInterface(stream);

    for await (const line of rl) {
        analyzer.addLine(line);
    }
}

processInput(inputStream);
