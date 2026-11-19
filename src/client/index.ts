import amqp from "amqplib";
import process from "node:process";
import {
  clientWelcome,
  commandStatus,
  getInput,
  getMaliciousLog,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { declareAndBind, SimpleQueueType, subscribeJSON } from "../internal/pubsub/consume.js";
import { ExchangePerilDirect, ExchangePerilTopic, PauseKey, ArmyMovesPrefix } from "../internal/routing/routing.js";
import { handlerPause, handlerMove, handlerWar } from "./handlers.js";
import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import { publishGameLog, publishJSON } from "../internal/pubsub/publish.js";
import type { RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { WarRecognitionsPrefix } from "../internal/routing/routing.js";

async function main() {
 const rabbitConnString = "amqp://guest:guest@localhost:5672/";
 const conn = await amqp.connect(rabbitConnString);
 if (conn){
  console.log("Connection was successful");
 }


 const username = await clientWelcome();
 await declareAndBind(
  conn,
  ExchangePerilDirect, 
  `pause.${username}`,
  PauseKey,
  SimpleQueueType.Transient,
);
const gameState = new GameState(username);
const confirmChannel = await conn.createConfirmChannel();
await subscribeJSON(
  conn,
  ExchangePerilTopic,
  `${ArmyMovesPrefix}.${username}`,
  `${ArmyMovesPrefix}.*`,
  SimpleQueueType.Transient,
  (move: ArmyMove) => handlerMove(gameState, confirmChannel, move),
);
await subscribeJSON(
  conn,
  ExchangePerilTopic,
  "war",
  `${WarRecognitionsPrefix}.*`,
  SimpleQueueType.Durable,
  (rw: RecognitionOfWar) => handlerWar(gameState, confirmChannel, rw),
);

 while(true){
  const words = await getInput();
  if(words.length === 0){
    continue;
  }
  const command = words[0];

  switch(command){
    case "spawn":
      try{
        commandSpawn(gameState, words);
      } catch (err){
        console.log((err as Error).message);
      }
      break;
    case "move":
      try{
        const move = commandMove(gameState, words);
        await publishJSON(
          confirmChannel,
          ExchangePerilTopic,
          `${ArmyMovesPrefix}.${username}`,
          move,
        );
        console.log("Move successful");
        
      } catch (err){
        console.log((err as Error).message);
      }
      break;
    case "status":
      await commandStatus(gameState);
      break;
    case "help":
      printClientHelp();
      break;
    case "spam":
        const n = parseInt(words[1] ?? "", 10);
        if (Number.isNaN(n)) {
          console.log("usage: spam <n>");
          break;
        }
        for (let i = 0; i < n; i++) {
          await publishGameLog(confirmChannel, username, getMaliciousLog());
        }

      break;
    case "quit":
      printQuit();
      await conn.close();
      process.exit(0);
    
    default:
      console.log("I don´t understand that command");
        }

 }

}


main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
