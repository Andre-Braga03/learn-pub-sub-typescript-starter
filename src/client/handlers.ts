import type { GameState } from "../internal/gamelogic/gamestate.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import type { AckType } from "../internal/pubsub/consume.js";
import type { ConfirmChannel } from "amqplib";
import type { ArmyMove, RecognitionOfWar } from "../internal/gamelogic/gamedata.js";
import { publishGameLog, publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilTopic, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { handleWar, WarOutcome } from "../internal/gamelogic/war.js";


export function handlerPause(gs: GameState, ps: PlayingState): AckType {
    handlePause(gs, ps);
    console.log("> ")
    return "Ack";

}

export async function handlerMove(
    gs: GameState,
    ch: ConfirmChannel,
    move: ArmyMove,
  ): Promise<AckType> {
    const outcome = handleMove(gs, move);
    if (outcome === MoveOutcome.MakeWar) {
      const rw: RecognitionOfWar = {
        attacker: move.player,
        defender: gs.getPlayerSnap(),
      };
      try{
        await publishJSON(
          ch,
          ExchangePerilTopic,
          `${WarRecognitionsPrefix}.${gs.getUsername()}`,
          rw,
        );
        return "Ack"
      } catch (err){
        console.error(err);
        return "NackRequeue";
      }
    }

    process.stdout.write("> ");
    if (outcome === MoveOutcome.Safe) {
      return "Ack";
    }
    return "NackDiscard";
  }


export async function handlerWar(
  gs: GameState,
  ch: ConfirmChannel,
  rw: RecognitionOfWar,
): Promise<AckType> {
  const resolution = handleWar(gs, rw);
  let message: string | undefined;

  switch (resolution.result) {
    case WarOutcome.NotInvolved:
      process.stdout.write("> ");
      return "NackRequeue";
    case WarOutcome.NoUnits:
      process.stdout.write("> ");
      return "NackDiscard";
    case WarOutcome.OpponentWon:
    case WarOutcome.YouWon:
      message = `${resolution.winner} won a war against ${resolution.loser}`;
      break;
    case WarOutcome.Draw:
      message = `A war between ${resolution.attacker} and ${resolution.defender} resulted in a draw`;
      break;
    default:
      console.error("Unknown war outcome");
      process.stdout.write("> ");
      return "NackDiscard";
  }

  try {
    await publishGameLog(ch, gs.getUsername(), message);
    process.stdout.write("> ");
    return "Ack";
  } catch (err) {
    console.error(err);
    return "NackRequeue";
  }
}
  