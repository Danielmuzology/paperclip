import { Writable } from "node:stream";
import pino from "pino";
import { describe, expect, it } from "vitest";
import { httpSecretRedactionPaths } from "../middleware/logger.js";

describe("HTTP credential log redaction", () => {
  it("redacts linked-work control and callback secrets from serialized request headers", () => {
    let serialized = "";
    const destination = new Writable({
      write(chunk, _encoding, callback) {
        serialized += chunk.toString();
        callback();
      },
    });
    const testLogger = pino({ redact: [...httpSecretRedactionPaths] }, destination);
    testLogger.info({
      req: {
        headers: {
          authorization: "Bearer AUTH_SENTINEL",
          "x-paperclip-linked-work-control-secret": "CONTROL_SENTINEL",
          "x-paperclip-webhook-secret": "CALLBACK_SENTINEL",
          "x-request-id": "visible-request-id",
        },
      },
    }, "credential test");
    expect(serialized).not.toMatch(/AUTH_SENTINEL|CONTROL_SENTINEL|CALLBACK_SENTINEL/u);
    expect(serialized).toContain("visible-request-id");
    expect(serialized).toContain("[Redacted]");
  });
});
