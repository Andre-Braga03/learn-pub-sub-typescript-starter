import type { ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";
import type { GameLog } from "../gamelogic/logs.js";
import { ExchangePerilTopic, GameLogSlug } from "../routing/routing.js";

export async function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void>{
const payload = Buffer.from(JSON.stringify(value));
ch.publish(exchange, routingKey, payload, {
  contentType: "application/json",
});
}

export async function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {

  const payload = Buffer.from(encode(value));
  ch.publish(exchange, routingKey, payload, {
    contentType: "application/x-msgpack",
  });
}

export async function publishGameLog(
  ch: ConfirmChannel,
  username: string,
  message: string,
): Promise<void> {
  const gameLog: GameLog = {
    currentTime: new Date(),
    message,
    username,
  };

  await publishMsgPack(
    ch,
    ExchangePerilTopic,
    `${GameLogSlug}.${username}`,
    gameLog,
  );
}