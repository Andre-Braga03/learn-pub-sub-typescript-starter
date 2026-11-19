import type { GameState } from "../internal/gamelogic/gamestate.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { handleMove } from "../internal/gamelogic/move.js";
import type { ArmyMove } from "../internal/gamelogic/gamedata.js";

export function handlerPause(gs: GameState, ps: PlayingState): void {
    handlePause(gs, ps);
    console.log("> ")

}

export function handlerMove(gs: GameState, am: ArmyMove): void {
    handleMove(gs, am);
    console.log("> ")
}