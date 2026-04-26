#!/usr/bin/env npx tsx
/**
 * pi-otel-backfill — Import historical Pi session files into Laminar as OpenTelemetry traces.
 *
 * Reads Pi's append-only JSONL session files, reconstructs the span hierarchy
 * (session → agent → turn → tool), sets gen_ai.* + lmnr.* attributes, and
 * exports via OTLP/HTTP.
 *
 * Usage:
 *   pi-otel-backfill [options]
 *
 * Options:
 *   --sessions-dir <path>    Session directory (default: ~/.pi/agent/sessions)
 *   --endpoint <url>         OTLP/HTTP traces endpoint (default: $OTEL_EXPORTER_OTLP_TRACES_ENDPOINT or http://localhost:8000/v1/traces)
 *   --api-key <key>          Laminar project API key (default: $LMNR_PROJECT_API_KEY, or extracted from $OTEL_EXPORTER_OTLP_HEADERS)
 *   --service-name <name>    Override service.name for all traces (default: derived from session directory name)
 *   --dry-run                Parse and report but don't export
 *   --force                  Re-export already-processed sessions
 *   --verbose                Show per-session details
 *   --help                   Show this help
 */

import { createHash } from "node:crypto";
import {
	existsSync,
	readdirSync,
	readFileSync,
	statSync,
	writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { basename, join, resolve } from "node:path";

// ─── OTel SDK imports ────────────────────────────────────────────
import {
	context,
	SpanStatusCode,
	type Tracer,
	trace,
} from "@opentelemetry/api";
import { OTLPTraceExporter } from "@opentelemetry/exporter-trace-otlp-proto";
import { resourceFromAttributes } from "@opentelemetry/resources";
import {
	BasicTracerProvider,
	BatchSpanProcessor,
} from "@opentelemetry/sdk-trace-base";

// ─── Types ───────────────────────────────────────────────────────

interface SessionEntry {
	type: string;
	id?: string;
	parentId?: string | null;
	timestamp?: string | number;
	version?: number;
	cwd?: string;
	// model_change
	provider?: string;
	modelId?: string;
	// message
	message?: MessagePayload;
	// custom
	customType?: string;
}

interface MessagePayload {
	role: string;
	content?: unknown;
	provider?: string;
	model?: string;
	api?: string;
	usage?: UsagePayload;
	stopReason?: string;
	timestamp?: number;
	toolCallId?: string;
	toolName?: string;
	isError?: boolean;
}

interface UsagePayload {
	input: number;
	output: number;
	cacheRead: number;
	cacheWrite: number;
	totalTokens: number;
	cost: {
		input: number;
		output: number;
		cacheRead: number;
		cacheWrite: number;
		total: number;
	};
}

interface ToolCallBlock {
	type: "toolCall";
	id: string;
	name: string;
	arguments?: Record<string, unknown>;
}

interface Turn {
	assistantEntry: SessionEntry;
	toolCalls: ToolCallBlock[];
	toolResults: SessionEntry[];
	provider?: string;
	model?: string;
	usage?: UsagePayload;
	stopReason?: string;
	startTime: number;
	endTime: number;
}

interface AgentInteraction {
	userEntry: SessionEntry;
	turns: Turn[];
	startTime: number;
	endTime: number;
}

interface ParsedSession {
	sessionId: string;
	cwd?: string;
	startTime: number;
	endTime: number;
	agents: AgentInteraction[];
	modelChanges: Array<{ provider: string; modelId: string; timestamp: number }>;
}

interface MarkerData {
	version: number;
	exported: Record<string, { hash: string; exportedAt: string; spans: number }>;
}

interface BackfillStats {
	sessionsFound: number;
	sessionsSkipped: number;
	sessionsExported: number;
	sessionsFailed: number;
	totalSpans: number;
	totalTurns: number;
	totalToolCalls: number;
}

// ─── CLI ─────────────────────────────────────────────────────────

function parseArgs(argv: string[]): {
	sessionsDir: string;
	endpoint: string;
	apiKey: string;
	serviceName?: string;
	dryRun: boolean;
	force: boolean;
	verbose: boolean;
	help: boolean;
} {
	const args = argv.slice(2);
	let sessionsDir = "";
	let endpoint = "";
	let apiKey = "";
	let serviceName: string | undefined;
	let dryRun = false;
	let force = false;
	let verbose = false;
	let help = false;

	for (let i = 0; i < args.length; i++) {
		switch (args[i]) {
			case "--sessions-dir":
				sessionsDir = args[++i] ?? "";
				break;
			case "--endpoint":
				endpoint = args[++i] ?? "";
				break;
			case "--api-key":
				apiKey = args[++i] ?? "";
				break;
			case "--service-name":
				serviceName = args[++i] ?? "";
				break;
			case "--dry-run":
				dryRun = true;
				break;
			case "--force":
				force = true;
				break;
			case "--verbose":
			case "-v":
				verbose = true;
				break;
			case "--help":
			case "-h":
				help = true;
				break;
			default:
				console.error(`Unknown option: ${args[i]}`);
				process.exit(1);
		}
	}

	// Defaults from env
	if (!sessionsDir) {
		sessionsDir = join(homedir(), ".pi", "agent", "sessions");
	}
	if (!endpoint) {
		endpoint =
			process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT ??
			"http://localhost:8000/v1/traces";
	}
	if (!apiKey) {
		apiKey = process.env.LMNR_PROJECT_API_KEY ?? "";
		if (!apiKey) {
			// Try extracting from OTEL_EXPORTER_OTLP_HEADERS
			const headers = process.env.OTEL_EXPORTER_OTLP_HEADERS ?? "";
			const match = headers.match(/authorization=Bearer\s+(\S+)/i);
			if (match) apiKey = match[1];
		}
	}

	return {
		sessionsDir: resolve(sessionsDir),
		endpoint,
		apiKey,
		serviceName,
		dryRun,
		force,
		verbose,
		help,
	};
}

function printHelp(): void {
	const lines = [];
	const src = readFileSync(new URL(import.meta.url).pathname, "utf8");
	const docBlock = src.match(/\/\*\*([\s\S]*?)\*\//);
	if (docBlock) {
		for (const line of docBlock[1].split("\n")) {
			const trimmed = line.replace(/^\s*\*\s?/, "");
			lines.push(trimmed);
		}
	}
	console.log(lines.join("\n"));
}

// ─── Session parsing ─────────────────────────────────────────────

function parseTimestamp(ts: string | number | undefined): number {
	if (ts === undefined) return 0;
	if (typeof ts === "number") return ts;
	return new Date(ts).getTime();
}

function findAllSessionFiles(dir: string): string[] {
	const files: string[] = [];
	if (!existsSync(dir)) return files;

	function walk(current: string): void {
		for (const entry of readdirSync(current)) {
			const full = join(current, entry);
			const stat = statSync(full);
			if (stat.isDirectory()) {
				walk(full);
			} else if (entry.endsWith(".jsonl")) {
				files.push(full);
			}
		}
	}

	walk(dir);
	return files.sort();
}

function parseSessionFile(filePath: string): ParsedSession | null {
	const raw = readFileSync(filePath, "utf8");
	const lines = raw.split("\n").filter((l) => l.trim());
	if (lines.length === 0) return null;

	const entries: SessionEntry[] = [];
	for (const line of lines) {
		try {
			entries.push(JSON.parse(line));
		} catch {
			// Skip malformed lines
		}
	}

	if (entries.length === 0) return null;

	// First entry should be the session header
	const header = entries[0];
	if (header.type !== "session") return null;

	const sessionId = header.id ?? basename(filePath, ".jsonl");
	const cwd = header.cwd;
	const sessionStartTime = parseTimestamp(header.timestamp);

	// Collect model changes
	const modelChanges: ParsedSession["modelChanges"] = [];

	// Separate messages from metadata
	const messages: SessionEntry[] = [];
	let currentProvider: string | undefined;
	let currentModel: string | undefined;

	for (const entry of entries) {
		if (entry.type === "model_change") {
			if (entry.provider) currentProvider = entry.provider;
			if (entry.modelId) currentModel = entry.modelId;
			modelChanges.push({
				provider: entry.provider ?? "",
				modelId: entry.modelId ?? "",
				timestamp: parseTimestamp(entry.timestamp),
			});
		} else if (entry.type === "message" && entry.message) {
			messages.push(entry);
		}
	}

	// Reconstruct agent interactions (user prompt → turns)
	const agents: AgentInteraction[] = [];
	let currentAgent: AgentInteraction | null = null;
	let currentTurn: Turn | null = null;

	for (const entry of messages) {
		const msg = entry.message!;
		const entryTime = parseTimestamp(entry.timestamp ?? msg.timestamp);

		if (msg.role === "user") {
			// Close previous turn & agent
			if (currentTurn && currentAgent) {
				currentAgent.turns.push(currentTurn);
				currentAgent.endTime = currentTurn.endTime;
			}
			if (currentAgent) {
				agents.push(currentAgent);
			}

			currentAgent = {
				userEntry: entry,
				turns: [],
				startTime: entryTime,
				endTime: entryTime,
			};
			currentTurn = null;
		} else if (msg.role === "assistant") {
			// Close previous turn
			if (currentTurn && currentAgent) {
				currentAgent.turns.push(currentTurn);
			}

			// Extract tool calls from content
			const toolCalls: ToolCallBlock[] = [];
			if (Array.isArray(msg.content)) {
				for (const block of msg.content) {
					if (
						typeof block === "object" &&
						block !== null &&
						(block as Record<string, unknown>).type === "toolCall"
					) {
						toolCalls.push(block as ToolCallBlock);
					}
				}
			}

			currentTurn = {
				assistantEntry: entry,
				toolCalls,
				toolResults: [],
				provider: msg.provider ?? currentProvider,
				model: msg.model ?? currentModel,
				usage: msg.usage,
				stopReason: msg.stopReason,
				startTime: entryTime,
				endTime: entryTime,
			};
		} else if (msg.role === "toolResult") {
			if (currentTurn) {
				currentTurn.toolResults.push(entry);
				currentTurn.endTime = Math.max(currentTurn.endTime, entryTime);
			}
		}
	}

	// Close final turn & agent
	if (currentTurn && currentAgent) {
		currentAgent.turns.push(currentTurn);
		currentAgent.endTime = currentTurn.endTime;
	}
	if (currentAgent) {
		agents.push(currentAgent);
	}

	// Compute session end time
	let sessionEndTime = sessionStartTime;
	for (const agent of agents) {
		if (agent.endTime > sessionEndTime) sessionEndTime = agent.endTime;
	}

	// Skip empty sessions (no turns at all)
	const totalTurns = agents.reduce((sum, a) => sum + a.turns.length, 0);
	if (totalTurns === 0) return null;

	return {
		sessionId,
		cwd,
		startTime: sessionStartTime,
		endTime: sessionEndTime,
		agents,
		modelChanges,
	};
}

// ─── OTel span generation ────────────────────────────────────────

function deriveServiceName(filePath: string, sessionsDir: string): string {
	// Fallback when cwd isn't available — decode the directory slug
	const rel = filePath.slice(sessionsDir.length + 1);
	const slugDir = rel.split("/")[0] ?? "";
	const decoded = slugDir.replace(/^--/, "").replace(/--$/, "");
	const parts = decoded.split("-").filter(Boolean);
	if (parts.length > 0) {
		const meaningful = parts.slice(-2).join("-");
		return meaningful.toLowerCase().replace(/[^a-z0-9._-]/g, "-");
	}
	return "pi-session";
}

function serviceNameFromCwd(cwd: string): string {
	// Use last 1-2 meaningful path segments after stripping home dir
	const parts = cwd.split("/").filter(Boolean);
	if (parts.length === 0) return "pi-session";
	const homeParts = homedir().split("/").filter(Boolean);
	let start = 0;
	for (let i = 0; i < homeParts.length && i < parts.length; i++) {
		if (parts[i] === homeParts[i]) start = i + 1;
		else break;
	}
	const meaningful = parts.slice(start);
	if (meaningful.length === 0) return "pi-session";
	const name = meaningful.slice(-2).join("-").toLowerCase()
		.replace(/[^a-z0-9._-]/g, "-").replace(/-+/g, "-").replace(/^[-_.]+|[-_.]+$/g, "");
	return name || "pi-session";
}

function previewText(value: string, maxChars: number): string {
	const normalized = value.replace(/\s+/g, " ").trim();
	if (normalized.length <= maxChars) return normalized;
	return `${normalized.slice(0, maxChars)}…`;
}

function exportSession(session: ParsedSession, tracer: Tracer): number {
	let spanCount = 0;

	// Root session span
	const sessionSpan = tracer.startSpan("pi.session", {
		startTime: session.startTime,
	});
	sessionSpan.setAttribute("pi.session.id", session.sessionId);
	if (session.cwd) {
		sessionSpan.setAttribute("pi.session.file", session.cwd);
	}

	// Laminar association properties — enables Sessions tab, metadata filtering, and tags
	sessionSpan.setAttribute("lmnr.association.properties.session_id", session.sessionId);
	const cwd = session.cwd ?? "";
	const project = cwd.split("/").filter(Boolean).pop() ?? "unknown";
	sessionSpan.setAttribute("lmnr.association.properties.metadata.cwd", cwd);
	sessionSpan.setAttribute("lmnr.association.properties.metadata.project", project);
	sessionSpan.setAttribute("lmnr.association.properties.tags", [project]);

	spanCount++;

	// Extract first user prompt for session name
	const firstUser = session.agents[0]?.userEntry.message;
	if (firstUser?.content) {
		const text =
			typeof firstUser.content === "string"
				? firstUser.content
				: Array.isArray(firstUser.content)
					? (firstUser.content as Array<{ type?: string; text?: string }>)
							.filter((b) => b.type === "text" && b.text)
							.map((b) => b.text)
							.join(" ")
					: "";
		if (text) {
			sessionSpan.updateName(`pi.session ${previewText(text, 50)}`);
		}
	}

	for (const agent of session.agents) {
		// Agent span
		const agentCtx = trace.setSpan(context.active(), sessionSpan);
		const agentSpan = tracer.startSpan(
			"pi.agent",
			{ startTime: agent.startTime },
			agentCtx,
		);
		spanCount++;

		for (let turnIdx = 0; turnIdx < agent.turns.length; turnIdx++) {
			const turn = agent.turns[turnIdx];

			// Turn span
			const turnCtx = trace.setSpan(context.active(), agentSpan);
			const turnSpan = tracer.startSpan(
				"pi.turn",
				{ startTime: turn.startTime },
				turnCtx,
			);
			turnSpan.setAttribute("pi.turn.index", turnIdx);
			turnSpan.setAttribute("lmnr.span.type", "LLM");
			spanCount++;

			// gen_ai.* attributes
			if (turn.provider) {
				turnSpan.setAttribute("gen_ai.system", turn.provider);
			}
			if (turn.model) {
				turnSpan.setAttribute("gen_ai.request.model", turn.model);
			}
			if (turn.usage) {
				turnSpan.setAttribute("gen_ai.usage.input_tokens", turn.usage.input);
				turnSpan.setAttribute("gen_ai.usage.output_tokens", turn.usage.output);
				if (turn.usage.cost) {
					turnSpan.setAttribute("gen_ai.usage.cost", turn.usage.cost.total);
					turnSpan.setAttribute(
						"gen_ai.usage.input_cost",
						turn.usage.cost.input,
					);
					turnSpan.setAttribute(
						"gen_ai.usage.output_cost",
						turn.usage.cost.output,
					);
				}
				if (turn.usage.cacheRead) {
					turnSpan.setAttribute(
						"gen_ai.usage.cache_read_input_tokens",
						turn.usage.cacheRead,
					);
				}
				if (turn.usage.cacheWrite) {
					turnSpan.setAttribute(
						"gen_ai.usage.cache_creation_input_tokens",
						turn.usage.cacheWrite,
					);
				}
			}

			// Tool spans
			const toolResultMap = new Map<string, SessionEntry>();
			for (const tr of turn.toolResults) {
				const id = tr.message?.toolCallId;
				if (id) toolResultMap.set(id, tr);
			}

			for (const tc of turn.toolCalls) {
				const toolCtx = trace.setSpan(context.active(), turnSpan);

				const toolName = tc.name;
				let toolSpanName = `pi.tool: ${toolName}`;
				if (toolName === "bash" && tc.arguments?.command) {
					toolSpanName = `pi.tool: bash(${previewText(String(tc.arguments.command), 120)})`;
				}

				const toolStart = turn.startTime; // best we have
				const toolSpan = tracer.startSpan(
					toolSpanName,
					{ startTime: toolStart },
					toolCtx,
				);
				toolSpan.setAttribute("pi.tool.name", toolName);
				toolSpan.setAttribute("pi.tool.call_id", tc.id);
				toolSpan.setAttribute("lmnr.span.type", "TOOL");

				// Laminar Input panel: tool arguments
				if (tc.arguments) {
					try {
						toolSpan.setAttribute("lmnr.span.input", JSON.stringify(tc.arguments));
					} catch { /* skip */ }
				}

				spanCount++;

				const result = toolResultMap.get(tc.id);
				if (result) {
					const resultTime = parseTimestamp(
						result.timestamp ?? result.message?.timestamp,
					);

					// Laminar Output panel: tool result content
					const resultContent = result.message?.content;
					if (resultContent !== undefined) {
						try {
							const out = typeof resultContent === "string"
								? resultContent
								: JSON.stringify(resultContent);
							// Cap at 64KB to avoid oversized span attributes
							toolSpan.setAttribute("lmnr.span.output",
								out.length > 65536 ? out.slice(0, 65536) + "\u2026" : out);
						} catch { /* skip */ }
					}

					if (result.message?.isError) {
						toolSpan.setStatus({
							code: SpanStatusCode.ERROR,
							message: "tool_result error",
						});
					}
					toolSpan.end(resultTime || turn.endTime);
				} else {
					toolSpan.end(turn.endTime);
				}
			}

			// turn_end event (matching live telemetry structure)
			turnSpan.addEvent("turn_end", {
				"pi.event.type": "turn_end",
				"pi.turn.index": turnIdx,
				"pi.turn.tool_results": turn.toolResults.length,
				"pi.message.stop_reason": turn.stopReason ?? "",
			});
			turnSpan.end(turn.endTime);
		}

		// agent_end
		const lastTurn = agent.turns[agent.turns.length - 1];
		agentSpan.addEvent("agent_end", {
			"pi.event.type": "agent_end",
			"pi.message.stop_reason": lastTurn?.stopReason ?? "",
		});
		agentSpan.end(agent.endTime);
	}

	sessionSpan.end(session.endTime);
	return spanCount;
}

// ─── Marker file (idempotency) ───────────────────────────────────

function getMarkerPath(sessionsDir: string): string {
	return join(sessionsDir, ".otel-backfill-marker.json");
}

function loadMarker(sessionsDir: string): MarkerData {
	const path = getMarkerPath(sessionsDir);
	if (!existsSync(path)) {
		return { version: 1, exported: {} };
	}
	try {
		return JSON.parse(readFileSync(path, "utf8"));
	} catch {
		return { version: 1, exported: {} };
	}
}

function saveMarker(sessionsDir: string, marker: MarkerData): void {
	writeFileSync(
		getMarkerPath(sessionsDir),
		JSON.stringify(marker, null, 2) + "\n",
	);
}

function fileHash(filePath: string): string {
	const stat = statSync(filePath);
	return createHash("sha256")
		.update(`${filePath}:${stat.size}:${stat.mtimeMs}`)
		.digest("hex")
		.slice(0, 16);
}

// ─── Main ────────────────────────────────────────────────────────

async function main(): Promise<void> {
	const opts = parseArgs(process.argv);

	if (opts.help) {
		printHelp();
		process.exit(0);
	}

	if (!existsSync(opts.sessionsDir)) {
		console.error(`❌ Sessions directory not found: ${opts.sessionsDir}`);
		process.exit(1);
	}

	if (!opts.apiKey && !opts.dryRun) {
		console.error(
			"❌ No API key. Set --api-key, $LMNR_PROJECT_API_KEY, or $OTEL_EXPORTER_OTLP_HEADERS",
		);
		process.exit(1);
	}

	console.log("pi-otel-backfill");
	console.log(`  sessions:  ${opts.sessionsDir}`);
	console.log(`  endpoint:  ${opts.endpoint}`);
	console.log(
		`  api-key:   ${opts.apiKey ? opts.apiKey.slice(0, 8) + "…" : "(none)"}`,
	);
	console.log(`  dry-run:   ${opts.dryRun}`);
	console.log(`  force:     ${opts.force}`);
	console.log();

	// Find all session files
	const allFiles = findAllSessionFiles(opts.sessionsDir);
	console.log(`Found ${allFiles.length} session files`);

	// Load marker
	const marker = opts.force
		? ({ version: 1, exported: {} } as MarkerData)
		: loadMarker(opts.sessionsDir);

	// Filter to unexported sessions
	const toProcess: string[] = [];
	let skipped = 0;
	for (const f of allFiles) {
		const hash = fileHash(f);
		const existing = marker.exported[f];
		if (existing && existing.hash === hash) {
			skipped++;
			continue;
		}
		toProcess.push(f);
	}

	console.log(`  ${skipped} already exported, ${toProcess.length} to process`);
	console.log();

	if (toProcess.length === 0) {
		console.log("Nothing to do.");
		process.exit(0);
	}

	const stats: BackfillStats = {
		sessionsFound: allFiles.length,
		sessionsSkipped: skipped,
		sessionsExported: 0,
		sessionsFailed: 0,
		totalSpans: 0,
		totalTurns: 0,
		totalToolCalls: 0,
	};

	// Group files by project directory for service.name routing
	const byProject = new Map<string, string[]>();
	for (const f of toProcess) {
		const rel = f.slice(opts.sessionsDir.length + 1);
		const projectSlug = rel.split("/")[0] ?? "unknown";
		const list = byProject.get(projectSlug) ?? [];
		list.push(f);
		byProject.set(projectSlug, list);
	}

	for (const [projectSlug, files] of byProject) {
		// Try to get service name from the first parseable session's cwd
		let serviceName = opts.serviceName ?? "";
		if (!serviceName) {
			for (const f of files) {
				const probe = parseSessionFile(f);
				if (probe?.cwd) {
					serviceName = serviceNameFromCwd(probe.cwd);
					break;
				}
			}
			if (!serviceName) {
				serviceName = deriveServiceName(files[0], opts.sessionsDir);
			}
		}
		console.log(
			`─── ${projectSlug} (service: ${serviceName}, ${files.length} sessions) ───`,
		);

		// Create a trace provider per project (unique service.name)
		let provider: BasicTracerProvider | undefined;
		let tracer: Tracer;

		if (!opts.dryRun) {
			const headers: Record<string, string> = {};
			if (opts.apiKey) {
				headers["authorization"] = `Bearer ${opts.apiKey}`;
			}

			const exporter = new OTLPTraceExporter({
				url: opts.endpoint,
				headers,
			});

			provider = new BasicTracerProvider({
				resource: resourceFromAttributes({
					"service.name": serviceName,
					"service.version": "backfill",
				}),
				spanProcessors: [new BatchSpanProcessor(exporter)],
			});

			tracer = provider.getTracer("pi-otel-backfill", "0.2.0");
		} else {
			// Dry run: use a no-op tracer that still lets us count
			tracer = trace.getTracer("pi-otel-backfill-dry");
		}

		for (const filePath of files) {
			const session = parseSessionFile(filePath);
			if (!session) {
				if (opts.verbose)
					console.log(`  skip (empty/invalid): ${basename(filePath)}`);
				stats.sessionsFailed++;
				continue;
			}

			const turnCount = session.agents.reduce((s, a) => s + a.turns.length, 0);
			const toolCount = session.agents.reduce(
				(s, a) => s + a.turns.reduce((ts, t) => ts + t.toolCalls.length, 0),
				0,
			);

			if (opts.dryRun) {
				const model = session.agents[0]?.turns[0]?.model ?? "unknown";
				console.log(
					`  [dry] ${basename(filePath).slice(0, 40)}  turns=${turnCount}  tools=${toolCount}  model=${model}`,
				);
				stats.sessionsExported++;
				stats.totalTurns += turnCount;
				stats.totalToolCalls += toolCount;
				continue;
			}

			try {
				const spanCount = exportSession(session, tracer);
				stats.sessionsExported++;
				stats.totalSpans += spanCount;
				stats.totalTurns += turnCount;
				stats.totalToolCalls += toolCount;

				// Mark as exported
				marker.exported[filePath] = {
					hash: fileHash(filePath),
					exportedAt: new Date().toISOString(),
					spans: spanCount,
				};

				if (opts.verbose) {
					console.log(
						`  ✓ ${basename(filePath).slice(0, 40)}  spans=${spanCount}  turns=${turnCount}  tools=${toolCount}`,
					);
				}
			} catch (err) {
				stats.sessionsFailed++;
				console.error(
					`  ✗ ${basename(filePath)}: ${err instanceof Error ? err.message : String(err)}`,
				);
			}
		}

		// Flush this project's provider before moving on
		if (provider) {
			try {
				await provider.forceFlush();
				await provider.shutdown();
			} catch (err) {
				console.error(
					`  ⚠ flush error for ${serviceName}: ${err instanceof Error ? err.message : String(err)}`,
				);
			}
		}

		console.log();
	}

	// Save marker (unless dry run)
	if (!opts.dryRun) {
		saveMarker(opts.sessionsDir, marker);
		console.log(`Marker saved: ${getMarkerPath(opts.sessionsDir)}`);
	}

	console.log();
	console.log("─── Summary ───");
	console.log(`  Sessions found:    ${stats.sessionsFound}`);
	console.log(`  Already exported:  ${stats.sessionsSkipped}`);
	console.log(`  Exported now:      ${stats.sessionsExported}`);
	console.log(`  Failed/empty:      ${stats.sessionsFailed}`);
	console.log(`  Total spans:       ${stats.totalSpans}`);
	console.log(`  Total turns:       ${stats.totalTurns}`);
	console.log(`  Total tool calls:  ${stats.totalToolCalls}`);
}

main().catch((err) => {
	console.error("Fatal:", err);
	process.exit(1);
});
