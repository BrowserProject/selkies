// @ts-check
/** How often a connected peer reads its path again, for browsers that report no pair change. */
export const PATH_RECHECK_MS = 5000;
/** What a peer reports while its path reads nothing: the server then holds nothing on it. */
export const PATH_UNKNOWN = "unknown";

/**
 * Keeps the server told which protocol carries one connected peer:
 * `_path,udp|tcp|tls` (`WebRTCClient.pathProtocol`), or `_path,unknown` while
 * that reads nothing, sent whenever it differs from the value last sent. The server's static hold (Selkies patch 0018)
 * holds a display only while every peer reports UDP, so the report follows
 * the selected candidate pair for as long as the peer stays connected: read
 * on `start`, on every `selectedcandidatepairchange` of the ICE transport
 * where the browser fires one, on `check` (the data channel opening) and
 * every PATH_RECHECK_MS.
 */
export class PathReporter {
	/**
	 * @param {{pathProtocol: function(): Promise<?string>, dataChannelOpen: function(): boolean,
	 *   sendDataChannelMessage: function(string): void, iceTransport: function(): ?EventTarget}} client
	 * @param {number} [recheckMs]
	 */
	constructor(client, recheckMs = PATH_RECHECK_MS) {
		this.client = client;
		this.recheckMs = recheckMs;
		/** The protocol last sent since `start`, null before the first report. @type {string|null} */
		this.sent = null;
		this._generation = 0;
		this._timer = null;
		this._transport = null;
		this._onChange = () => { this.check(); };
	}

	/** Reports for a peer that (re)connected: the server hears its path again. */
	start() {
		this.stop();
		this.sent = null;
		this._transport = this.client.iceTransport();
		if (this._transport) this._transport.addEventListener("selectedcandidatepairchange", this._onChange);
		this._timer = setInterval(this._onChange, this.recheckMs);
		this.check();
	}

	/** Stops the rechecks and the pair listener; a read still in flight sends nothing. */
	stop() {
		this._generation++;
		if (this._timer !== null) {
			clearInterval(this._timer);
			this._timer = null;
		}
		if (this._transport) {
			this._transport.removeEventListener("selectedcandidatepairchange", this._onChange);
			this._transport = null;
		}
	}

	/** Reads the path now and sends it when it changed. */
	async check() {
		if (this._timer === null || !this.client.dataChannelOpen()) return;
		const generation = this._generation;
		let protocol = null;
		try { protocol = await this.client.pathProtocol(); } catch (e) { /* stats not readable */ }
		protocol = protocol || PATH_UNKNOWN;
		if (generation !== this._generation || protocol === this.sent) return;
		if (!this.client.dataChannelOpen()) return;
		try {
			this.client.sendDataChannelMessage(`_path,${protocol}`);
			this.sent = protocol;
		} catch (e) { /* the channel closed meanwhile; the next check sends again */ }
	}
}
