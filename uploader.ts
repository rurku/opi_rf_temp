import { EOL } from "os";
import IMessage from "./message";

export class Uploader {
    public async addMessage(message: IMessage) {
        // do nothing
        process.stdout.write(`timestamp ${message.timestamp} payload ${message.payload} ${EOL}`);
    }
}

export default Uploader;
