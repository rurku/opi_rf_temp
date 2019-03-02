import { EOL } from "os";
import IMessage from "./message";
import Reading from "./reading";

export class Uploader {
    public async addReading(reading: Reading) {
        // do nothing
        process.stdout.write(`${JSON.stringify(reading)}\n`);
    }
}

export default Uploader;
