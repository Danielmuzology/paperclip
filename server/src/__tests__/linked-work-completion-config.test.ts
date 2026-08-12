import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { loadConfig } from "../config.js";

const managedKeys = [
  "PAPERCLIP_LINKED_WORK_CALLBACK_URL",
  "PAPERCLIP_LINKED_WORK_CALLBACK_SECRET",
  "PAPERCLIP_LINKED_WORK_CONTROL_SECRET",
  "PAPERCLIP_TAILNET_BIND_HOST",
] as const;

const prior = new Map<string, string | undefined>();

beforeEach(() => {
  for (const key of managedKeys) {
    prior.set(key, process.env[key]);
    delete process.env[key];
  }
  process.env.PAPERCLIP_TAILNET_BIND_HOST = "100.64.0.1";
});

afterEach(() => {
  for (const key of managedKeys) {
    const value = prior.get(key);
    if (value === undefined) delete process.env[key];
    else process.env[key] = value;
  }
  prior.clear();
});

function configure(url = "http://muzorg-control-plane:4300/integrations/paperclip/linked-work/completion"): void {
  process.env.PAPERCLIP_LINKED_WORK_CALLBACK_URL = url;
  process.env.PAPERCLIP_LINKED_WORK_CALLBACK_SECRET = "c".repeat(32);
  process.env.PAPERCLIP_LINKED_WORK_CONTROL_SECRET = "t".repeat(32);
}

describe("linked-work completion config", () => {
  it("accepts the exact three-key contract", () => {
    configure();
    expect(loadConfig()).toMatchObject({
      linkedWorkCompletionCallbackUrl:
        "http://muzorg-control-plane:4300/integrations/paperclip/linked-work/completion",
      linkedWorkCompletionCallbackSecret: "c".repeat(32),
      linkedWorkCompletionControlSecret: "t".repeat(32),
    });
  });

  it("requires all three keys and minimum-length secrets", () => {
    process.env.PAPERCLIP_LINKED_WORK_CALLBACK_URL =
      "https://origin.example.test/integrations/paperclip/linked-work/completion";
    expect(() => loadConfig()).toThrow(/configured together/u);

    configure();
    process.env.PAPERCLIP_LINKED_WORK_CALLBACK_SECRET = "short";
    expect(() => loadConfig()).toThrow(/CALLBACK_SECRET must be at least 32/u);

    configure();
    process.env.PAPERCLIP_LINKED_WORK_CONTROL_SECRET = "short";
    expect(() => loadConfig()).toThrow(/CONTROL_SECRET must be at least 32/u);
  });

  it.each([
    "not-a-url-with-password=must-not-be-reflected",
    "ftp://origin.example.test/integrations/paperclip/linked-work/completion",
    "https://user:password@origin.example.test/integrations/paperclip/linked-work/completion",
    "https://origin.example.test/integrations/paperclip/linked-work/completion?next=1",
    "https://origin.example.test/integrations/paperclip/linked-work/completion#fragment",
    "https://origin.example.test/integrations/paperclip/linked-work/completion/extra",
  ])("rejects a noncanonical callback URL: %s", (url) => {
    configure(url);
    expect(() => loadConfig()).toThrow(/exact supported completion route/u);
  });
});
