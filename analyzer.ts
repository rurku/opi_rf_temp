import Edge from "./edge";
import IMessage from "./message";

export class Analyzer {
    private preambleCounter = 0;
    private preambleWidth = 0;
    private lastEdge: Edge;
    private lastWidth: number | null = null;
    private bitWidth: number;
    private payload: number[];
    private timestamp: number;

    constructor(private callback: (m: IMessage) => void) {
        this.callback = callback;
    }

    public addLine(line: string) {
        const edge = parseInputLine(line);
        if (line !== "" && !edge) {
            process.stderr.write(`Invalid input line: ${line}`);
            return;
        }

        if (this.lastEdge) {
            const width = timeDiff(edge, this.lastEdge);
            if (this.preambleCounter < 8) {
                // search mode
                if (width == null) {
                    this.reset();
                } else if (this.preambleCounter % 2 === edge.level
                    && (this.preambleCounter === 0
                        || isWithinMargin(width, this.lastWidth))) {
                    this.preambleCounter ++;
                    this.preambleWidth += width;
                    if (this.preambleCounter === 8) {
                        this.bitWidth = this.preambleWidth / 8;
                        this.timestamp = edge.timestamp;
                        this.payload = [];
                    }
                }
            } else if (width == null || edge.level === 1) {
                // if it's after preamble then we analyze bits at the rising edge
                // lastWidth is high level, width is low level
                if (width !== null && isWithinMargin(this.bitWidth, this.lastWidth + width)) {
                    if (this.payload.length >= 8192) {// this is getting too long. reset.
                        this.reset();
                    } else {
                        this.payload.push(this.lastWidth > width ? 1 : 0);
                    }
                } else {
                    // if we get empty line or the cycle is not near the bit width then it's end of message
                    this.callback({payload: this.payload, timestamp: this.timestamp});
                    this.reset();
                }
            }
            this.lastWidth = width;
        }
        this.lastEdge = edge;
    }

    private reset() {
        this.preambleCounter = 0;
        this.preambleWidth = 0;
        this.lastEdge = null;
        this.lastWidth = 0;
    }
}

function isWithinMargin(a: number, b: number): boolean {
    // if shorter than 200 then return false because it's too short to reliably measure
    if (a < 200 || b < 200) {
        return false;
    }
    return Math.abs(a - b) / a < 0.2;
}

function parseInputLine(line: string): Edge {
    const regex = /^(?<timestamp>\d{10,19}) (?<seconds>\d{1,19}).(?<nanoseconds>\d{9}) (?<level>[01])$/;
    const match = regex.exec(line);
    if (match) {
        const input = new Edge();
        input.timestamp = parseInt(match.groups.timestamp, 10);
        input.seconds = parseInt(match.groups.seconds, 10);
        input.nanoseconds = parseInt(match.groups.nanoseconds, 10);
        input.level = parseInt(match.groups.level, 10);
        return input;
    }
}

// Returns time difference between two edges in nanoseconds.
// If the time difference is >= 1 second then returns null.
// the returned time difference is an absolute value, so arguments are interchangable.
function timeDiff(a: Edge, b: Edge): number | null {
    if (a == null || b == null) {
        return null;
    }

    const secDiff = a.seconds - b.seconds;
    if (Math.abs(secDiff) > 1) {
        return null;
    }
    const totalDiff = secDiff * 1e9 + (a.nanoseconds - b.nanoseconds);
    if (totalDiff >= 1e9) {
        return null;
    }
    return Math.abs(totalDiff);
}

export default Analyzer;
