// @ts-check
/** Keyframe nudges after connect until a first frame lands, then one decoder rebuild when H.264 arrived
 *  but nothing painted; nothing to nudge for a socket this browser cannot decode.
 * @param {{painted: boolean, stopped: boolean, ending: boolean, superseded: boolean, undecodable: boolean,
 *   pipelineActive: boolean, socketOpen: boolean, hidden: boolean, exhausted: boolean, chunksArrived: boolean,
 *   resetDone: boolean}} state
 * @returns {'stop'|'wait'|'nudge'|'reset'}
 */
export function decideFirstFrameRecovery(state) {
  if (state.painted || state.stopped || state.ending || state.superseded || state.undecodable
      || !state.pipelineActive || !state.socketOpen) return 'stop';
  if (state.hidden) return 'wait';
  if (!state.exhausted) return 'nudge';
  return state.chunksArrived && !state.resetDone ? 'reset' : 'stop';
}
