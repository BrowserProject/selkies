// @ts-check
/**
 * Pure ClickFix matcher: does text the remote page put into the clipboard
 * look like a command the page wants the visitor to run (Win+R, Terminal,
 * PowerShell)?
 *
 * The word lists come from the analyzer (`GET /analyzer/{vmId}/clipboard/rules`,
 * synced from LOLBAS, LOOBins and the SigmaHQ ClickFix rules). Fixed here is
 * only grammar that never changes: shell operators and launch prefixes, the
 * operating systems' own command interpreters, PowerShell's encoded, hidden
 * and invoke-expression syntax, the `#` comment and Unicode's space
 * characters. Without rules that structural half still runs.
 * @module
 */

/** The operating systems' own command and script interpreters. */
const INTERPRETERS = new Set(['cmd', 'powershell', 'pwsh', 'mshta', 'wscript', 'cscript', 'sh', 'bash', 'zsh', 'osascript']);
/** Words that run the word after them (and their own switches) as the command. */
const LAUNCH_PREFIXES = new Set(['start', 'call', 'sudo', 'doas', 'exec', 'nohup', 'env', 'command', 'conhost']);
/** Shell and cmd operators. A newline starts a command too. */
const OPERATORS = ['&&', '||', '$(', '|', '&', ';', '(', ')'];
const OPERATOR_SET = new Set(OPERATORS);
/** One token: a line break, an operator or a word (quoted parts included); other whitespace separates. */
const TOKEN = /\r\n?|\n|&&|\|\||\$\(|[|&;()]|(?:"[^"]*"?|'[^']*'?|[^\s"'&|;()$]+|\$(?!\())+/;
const QUOTED_PART = /"([^"]*)"?|'([^']*)'?|([^"']+)/g;
/** Operators that chain one command into another. */
const CHAIN_OPERATORS = new Set(['|', '&&', '||']);
/** Operators after which the next word is in command position. */
const COMMAND_OPENERS = new Set(['\n', '|', '&&', '||', '&', ';', '(', '$(']);

const SWITCH = /^(?:--?[a-z][\w-]*|\/[a-z?][a-z0-9:]{0,11})$/i;
const URLISH = /^(?:[a-z][a-z0-9+.-]*:\/\/|\\\\|(?:java|vb)script:)/i;
const PATHISH = /^(?:[a-z]:[\\/]|~[\\/]|\.{1,2}[\\/]|\/[^\s/]|%[\w()]+%|\$[\w{(])/i;
const SCRIPTISH = /[|;&$()`\\]|:\/\//;
/** An interpreter switch whose remaining words are the command it runs. */
const INLINE_COMMAND = /^(?:\/[ckr]|-c|-command)$/i;
/** cmd's own inline switches: a `%VAR%` in command position followed by one is an interpreter (`%ComSpec% /c`). */
const CMD_INLINE = /^\/[ckr]$/i;
const ENV_VARIABLE = /^%\w+%$/;
const EXECUTABLE_SUFFIX = /\.(?:exe|com|bat|cmd|ps1|vbs|wsf)$/i;
const POWERSHELL = /\b(?:powershell|pwsh)(?:\.exe)?\b/i;
const ENCODED_SWITCH = /\s[-/]e(?:c|n[a-z]*)?\s+['"]?[a-z0-9+/]{20,}/i;
const HIDDEN_SWITCH = /\s[-/]w(?:i[a-z]*)?\s+['"]?(?:h(?:i[a-z]*)?|1)\b/i;
const INVOKE_EXPRESSION = /\|\s*iex\b|\biex\s*[($'"]|\binvoke-expression\b|\bfrombase64string\b/i;
const BASE64_BLOB = /[A-Za-z0-9+/]{48,}={0,2}/;
const UNICODE_SPACE_RUN = /[\u00a0\u1680\u2000-\u200a\u202f\u205f\u3000]{3,}/;
const ASCII_SPACE_RUN = /\S {24,}\S/;
const COMMENT_AFTER_COMMAND = /\S\s+#/;
const PROSE_URL = /\S*:\/\/\S*/g;
const PROSE_SWITCH = /(^|\s)[-/]{1,2}[a-z][\w-]*/gi;
const WORD = /[\p{L}\p{N}][\p{L}\p{N}-]*/gu;
const ESCAPES = /[\^`]/g;

/** Longest text the matcher reads; a payload has to fit a Run box or a terminal line anyway. */
export const CLIPBOARD_SCAN_LIMIT = 1024 * 1024;
/** Most terms one match reports. */
export const CLIPBOARD_MATCH_TERMS = 8;
/** Deepest quoted script (`-c "..."`, `-e '...'`) read as a command line of its own. */
const NESTING_LIMIT = 2;
/** Every kind a match can carry, in report order. */
export const CLIPBOARD_MATCH_KINDS = ['command', 'fetchExec', 'obfuscated', 'comment', 'padding', 'lure'];
/** Kinds that make a match `bad`: the disguise, not the command. */
const BAD_KINDS = new Set(['obfuscated', 'comment', 'padding', 'lure']);

/**
 * @typedef {object} ClipboardRules
 * @property {Set<string>} commands Binary names (lowercase, no extension).
 * @property {Set<string>} lures Lure words of the fake-verification pages.
 * @property {string[]} padding Whitespace runs the padding rules match.
 */

/**
 * @typedef {object} ClipboardMatch
 * @property {'warn'|'bad'} severity
 * @property {string[]} kinds Subset of CLIPBOARD_MATCH_KINDS, in that order.
 * @property {string[]} terms Matched rule words (commands, lures), at most CLIPBOARD_MATCH_TERMS.
 */

/** @typedef {{op: string, word?: undefined, quoted?: undefined} | {op?: undefined, word: string, quoted: boolean}} Token */
/** @typedef {{word: string, quoted: boolean}} WordToken */
/** @typedef {{kinds: Set<string>, terms: Set<string>}} Found */

/** @param {unknown} value @returns {string[]} */
const stringList = (value) => (Array.isArray(value) ? value.filter((v) => typeof v === 'string' && v) : []);

/**
 * The analyzer's rule answer as matcher rules, or null when it is not one.
 * @param {unknown} raw `{commands, lures, padding}` string lists.
 * @returns {ClipboardRules|null}
 */
export function compileClipboardRules(raw) {
    if (!raw || typeof raw !== 'object') return null;
    const lists = /** @type {{commands?: unknown, lures?: unknown, padding?: unknown}} */ (raw);
    const commands = new Set(stringList(lists.commands).map((c) => c.toLowerCase()));
    const lures = new Set(stringList(lists.lures).map((c) => c.toLowerCase()));
    const padding = stringList(lists.padding).filter((p) => !p.trim());
    if (!commands.size && !lures.size && !padding.length) return null;
    return { commands, lures, padding };
}

/** Words, operators and quoted strings of one command line. @param {string} text @returns {Token[]} */
function lex(text) {
    /** @type {Token[]} */
    const tokens = [];
    const re = new RegExp(TOKEN.source, 'g');
    for (let m = re.exec(text); m; m = re.exec(text)) {
        const token = m[0];
        if (token === '\n' || token[0] === '\r') tokens.push({ op: '\n' });
        else if (OPERATOR_SET.has(token)) tokens.push({ op: token });
        else if (token.includes('"') || token.includes("'")) {
            tokens.push({ word: token.replace(QUOTED_PART, (/** @type {string} */ _, /** @type {string|undefined} */ dq, /** @type {string|undefined} */ sq, /** @type {string} */ bare) => dq ?? sq ?? bare.replace(ESCAPES, '')), quoted: true });
        } else {
            tokens.push({ word: token.replace(ESCAPES, ''), quoted: false });
        }
    }
    return tokens;
}

/** The binary a word names: basename, lowercase, without an executable suffix. @param {string} word */
function commandName(word) {
    if (word.includes('://')) return '';
    if (word.toLowerCase() === '%comspec%') return 'cmd';
    return (word.toLowerCase().split(/[\\/]/).pop() || '').replace(EXECUTABLE_SUFFIX, '');
}

/** @param {string} word */
const isPathy = (word) => /[\\/]/.test(word) || EXECUTABLE_SUFFIX.test(word);

/** An argument that makes a known word read as a command rather than prose. @param {WordToken} token */
function isCommandArgument(token) {
    if (token.quoted) return SCRIPTISH.test(token.word);
    return SWITCH.test(token.word) || URLISH.test(token.word) || PATHISH.test(token.word);
}

/**
 * Commands in command position, pipes into an interpreter and quoted scripts.
 * @param {string} text
 * @param {ClipboardRules|null} rules
 * @param {number} depth
 * @param {Found} found
 */
function scanCommands(text, rules, depth, found) {
    const tokens = lex(text);
    let start = 0;
    while (start < tokens.length) {
        let end = start;
        while (end < tokens.length && !tokens[end]?.op) end++;
        const before = (start > 0 ? tokens[start - 1]?.op : '\n') ?? '\n';
        const after = (end < tokens.length ? tokens[end]?.op : null) ?? null;
        if (COMMAND_OPENERS.has(before) && end > start) {
            scanSegment(/** @type {WordToken[]} */ (tokens.slice(start, end)), before, after, rules, depth, found);
        }
        start = end + 1;
    }
}

/**
 * @param {WordToken[]} segment
 * @param {string} before
 * @param {string|null} after
 * @param {ClipboardRules|null} rules
 * @param {number} depth
 * @param {Found} found
 */
function scanSegment(segment, before, after, rules, depth, found) {
    /** @param {number} index @returns {WordToken} */
    const tokenAt = (index) => /** @type {WordToken} */ (segment[index]);
    let at = 0;
    while (at < segment.length) {
        const token = tokenAt(at);
        const name = token.quoted ? '' : commandName(token.word);
        if (/^\w+=/.test(token.word) && !token.quoted) { at++; continue; }
        if (!LAUNCH_PREFIXES.has(name)) break;
        at++;
        while (at < segment.length && (SWITCH.test(tokenAt(at).word) || (tokenAt(at).quoted && !tokenAt(at).word))) at++;
    }
    if (at >= segment.length) return;
    const head = tokenAt(at);
    const args = segment.slice(at + 1);
    const cmdShaped = !head.quoted && ENV_VARIABLE.test(head.word) && args.length > 0 && CMD_INLINE.test(args[0]?.word ?? '');
    const name = cmdShaped ? 'cmd' : commandName(head.word);
    const interpreter = INTERPRETERS.has(name);
    if (!interpreter && !(rules && rules.commands.has(name))) return;
    if (interpreter && (before === '|' || after === '$(' || args.some((a) => /\$\(|`/.test(a.word)))) {
        found.kinds.add('fetchExec');
        found.terms.add(name);
    }
    const chained = CHAIN_OPERATORS.has(before) || (after !== null && CHAIN_OPERATORS.has(after)) || after === '$(' || after === '(';
    if (isPathy(head.word) || chained || args.some(isCommandArgument)) {
        found.kinds.add('command');
        found.terms.add(name);
    }
    if (depth >= NESTING_LIMIT) return;
    const inline = interpreter ? args.findIndex((a) => !a.quoted && INLINE_COMMAND.test(a.word)) : -1;
    if (inline !== -1 && inline + 1 < args.length && !args[inline + 1]?.quoted) {
        scanSegment(args.slice(inline + 1), '\n', after, rules, depth + 1, found);
    }
    for (const arg of args) {
        if (arg.quoted && /\s/.test(arg.word)) scanCommands(arg.word, rules, depth + 1, found);
    }
}

/** @param {string} text @param {boolean} flagged */
function hasObfuscation(text, flagged) {
    if (INVOKE_EXPRESSION.test(text)) return true;
    const shell = POWERSHELL.exec(text);
    if (shell) {
        const rest = text.slice(shell.index);
        if (ENCODED_SWITCH.test(rest) || HIDDEN_SWITCH.test(rest)) return true;
    }
    if (!flagged) return false;
    const blob = BASE64_BLOB.exec(text);
    return !!blob && /\d/.test(blob[0]) && /[a-z]/.test(blob[0]) && /[A-Z]/.test(blob[0]);
}

/** @param {string} text @param {ClipboardRules|null} rules @returns {string[]} */
function lureWords(text, rules) {
    /** @type {string[]} */
    const out = [];
    if (!rules || !rules.lures.size) return out;
    const prose = text.replace(PROSE_URL, ' ').replace(PROSE_SWITCH, ' ').toLowerCase();
    for (const word of new Set(prose.match(WORD) || [])) {
        if (rules.lures.has(word)) out.push(word);
    }
    return out;
}

/**
 * Whether `text` looks like a command a page wants the visitor to run.
 *
 * Flagged when a known binary sits in command position with an argument that
 * only a command line has (a switch, URL or path, a chain operator, a path to
 * the binary itself), when text is piped into an interpreter, or when
 * PowerShell's encoded, hidden-window or invoke-expression syntax appears.
 * The match is `bad` once the disguise shows: lure words, a `#` comment
 * after the command on a one-line payload, whitespace padding, or
 * obfuscation; otherwise it is `warn` (a docs page's `curl ... | sh`).
 * @param {string} text Clipboard text.
 * @param {ClipboardRules|null} rules Synced word lists, or null for the structural core only.
 * @returns {ClipboardMatch|null} Null when the text does not look like a command.
 */
export function matchClipboardCommand(text, rules) {
    if (typeof text !== 'string' || !text.trim()) return null;
    const scanned = text.length > CLIPBOARD_SCAN_LIMIT ? text.slice(0, CLIPBOARD_SCAN_LIMIT) : text;
    /** @type {Found} */
    const found = { kinds: new Set(), terms: new Set() };
    scanCommands(scanned, rules, 0, found);
    const unescaped = scanned.replace(ESCAPES, '');
    if (hasObfuscation(unescaped, found.kinds.size > 0)) found.kinds.add('obfuscated');
    if (!found.kinds.size) return null;

    const trimmed = scanned.trim();
    if (!/[\r\n]/.test(trimmed) && COMMENT_AFTER_COMMAND.test(trimmed)) found.kinds.add('comment');
    if (UNICODE_SPACE_RUN.test(scanned) || ASCII_SPACE_RUN.test(scanned)
        || (rules && rules.padding.some((p) => scanned.includes(p)))) {
        found.kinds.add('padding');
    }
    const lures = lureWords(scanned, rules);
    if (lures.length) found.kinds.add('lure');

    const kinds = CLIPBOARD_MATCH_KINDS.filter((k) => found.kinds.has(k));
    const terms = [...found.terms, ...lures].slice(0, CLIPBOARD_MATCH_TERMS);
    return { severity: kinds.some((k) => BAD_KINDS.has(k)) ? 'bad' : 'warn', kinds, terms };
}
