import IMessage from "./message";

export class Analyzer {
    constructor(private callback: (m: IMessage) => void) {
        this.callback = callback;
    }

    public addLine(line: string) {
        this.callback({timestamp: 1, payload: line});
    }
}

export default Analyzer;
