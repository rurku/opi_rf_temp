import commander = require("commander");
import fs = require("fs");
import readline = require("readline");

import Analyzer from "./analyzer";
import IMessage from "./message";
import Uploader from "./uploader";

commander.option("-i, --input-file <file>", "Input file")
    .parse(process.argv);

let inputStream: NodeJS.ReadableStream;

if (commander.inputFile) {
    inputStream = fs.createReadStream(commander.inputFile);
} else {
    inputStream = process.stdin;
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
