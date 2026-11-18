import amqp from "amqplib";
import process from "node:process";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect, PauseKey , GameLogSlug, ExchangePerilTopic} from "../internal/routing/routing.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import { declareAndBind , SimpleQueueType} from "../internal/pubsub/consume.js";


async function main() {
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
 if (conn){
  console.log("Connection was successful");
 }
const confirmChannel = await conn.createConfirmChannel();
await declareAndBind(
  conn,
  ExchangePerilTopic,
  GameLogSlug,
  `${GameLogSlug}.*`,
  SimpleQueueType.Durable,
)
printServerHelp();
while(true){
  const words = await getInput();
  if(words.length === 0){
    continue;
  }
  const command = words[0];
  switch (command)
{
  case "pause":
    console.log("Sending a pause message");
    await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, {isPaused: true});
    break;
  case "resume":
    console.log("Sending a resume message");
    await publishJSON(confirmChannel, ExchangePerilDirect, PauseKey, {isPaused: false});
    break;
   
  case "quit":
    console.log("Exiting...");
    return;
  
  default:
    console.log("I don´t understand that command")
}}

}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
