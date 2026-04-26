import { describe, expect, it } from "vitest";
import { buildTraceUrl, getOpenUrlCommand } from "../src/diagnostics/open-trace-command.js";

describe("open trace command utils", () => {
  it("builds trace url", () => {
    expect(buildTraceUrl("http://localhost:5667/project/abc-123/traces", "def456")).toBe("http://localhost:5667/project/abc-123/traces/def456");
    expect(buildTraceUrl("http://localhost:5667/project/abc-123/traces/", "def456")).toBe("http://localhost:5667/project/abc-123/traces/def456");
  });

  it("returns platform-specific open command", () => {
    expect(getOpenUrlCommand("darwin", "https://example.com")).toEqual({
      command: "open",
      args: ["https://example.com"],
    });

    expect(getOpenUrlCommand("win32", "https://example.com")).toEqual({
      command: "cmd",
      args: ["/c", "start", "", "https://example.com"],
    });

    expect(getOpenUrlCommand("linux", "https://example.com")).toEqual({
      command: "xdg-open",
      args: ["https://example.com"],
    });
  });
});
