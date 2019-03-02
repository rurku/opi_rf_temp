export default interface IMessage {
    timestamp: number;
    /**
     * Binary representation of the message, composed of 0's and 1's
     */
    payload: string;
}
