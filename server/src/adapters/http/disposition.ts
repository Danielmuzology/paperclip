import { z } from "zod";
import { redactSensitiveText } from "../../redaction.js";

export const HTTP_ADAPTER_DISPOSITION_MAX_BYTES = 16 * 1024;

const bindingSchema = z
  .object({
    runId: z.string().uuid(),
    companyId: z.string().uuid(),
    agentId: z.string().uuid(),
    issueId: z.string().uuid(),
  })
  .strict();

const dispositionBase = {
  summary: z.string().trim().min(1).max(1_000),
  evidenceSha256: z.string().regex(/^[a-f0-9]{64}$/u),
};

const dispositionSchema = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("done"), ...dispositionBase }).strict(),
  z
    .object({
      kind: z.literal("blocked"),
      ...dispositionBase,
      blockerIssueId: z.string().uuid(),
    })
    .strict(),
  z.object({ kind: z.literal("continue"), ...dispositionBase }).strict(),
  z
    .object({
      kind: z.literal("in_review"),
      ...dispositionBase,
      reviewerAgentId: z.string().uuid(),
    })
    .strict(),
]);

export const paperclipAdapterDispositionEnvelopeSchema = z
  .object({
    schemaVersion: z.literal("paperclip.adapter-disposition.v1"),
    status: z.literal("completed"),
    binding: bindingSchema,
    disposition: dispositionSchema,
  })
  .strict();

export type PaperclipAdapterDispositionEnvelope = z.infer<
  typeof paperclipAdapterDispositionEnvelopeSchema
>;

export class PaperclipAdapterDispositionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "PaperclipAdapterDispositionError";
  }
}

export async function readPaperclipAdapterDispositionResponse(
  response: Response,
  expected: PaperclipAdapterDispositionEnvelope["binding"],
): Promise<PaperclipAdapterDispositionEnvelope> {
  const contentType = response.headers.get("content-type")?.split(";", 1)[0]?.trim().toLowerCase();
  if (contentType !== "application/json") {
    throw new PaperclipAdapterDispositionError(
      "HTTP adapter disposition response must use application/json.",
    );
  }
  const declaredLength = response.headers.get("content-length");
  if (declaredLength !== null) {
    if (!/^\d+$/u.test(declaredLength)) {
      throw new PaperclipAdapterDispositionError(
        "HTTP adapter disposition response has an invalid content length.",
      );
    }
    if (Number(declaredLength) > HTTP_ADAPTER_DISPOSITION_MAX_BYTES) {
      throw new PaperclipAdapterDispositionError(
        "HTTP adapter disposition response exceeds the size limit.",
      );
    }
  }

  const bytes = await readBoundedBody(response);
  let value: unknown;
  try {
    value = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes));
  } catch {
    throw new PaperclipAdapterDispositionError(
      "HTTP adapter disposition response is not valid UTF-8 JSON.",
    );
  }
  const parsed = paperclipAdapterDispositionEnvelopeSchema.safeParse(value);
  if (!parsed.success) {
    throw new PaperclipAdapterDispositionError(
      "HTTP adapter disposition response does not match the strict schema.",
    );
  }
  if (
    parsed.data.binding.runId !== expected.runId ||
    parsed.data.binding.companyId !== expected.companyId ||
    parsed.data.binding.agentId !== expected.agentId ||
    parsed.data.binding.issueId !== expected.issueId
  ) {
    throw new PaperclipAdapterDispositionError(
      "HTTP adapter disposition response binding does not match the active run.",
    );
  }
  const summary = parsed.data.disposition.summary;
  if (
    summary !== summary.trim() ||
    /[\u0000-\u001f\u007f]/u.test(summary) ||
    sanitizeDispositionSummary(summary) !== summary
  ) {
    throw new PaperclipAdapterDispositionError(
      "HTTP adapter disposition summary contains unsafe text.",
    );
  }
  return parsed.data;
}

function sanitizeDispositionSummary(value: string): string {
  return redactSensitiveText(value)
    .replace(
      /\b[a-z][a-z0-9+.-]*:\/\/[^\s/@]+:[^\s/@]+@[^\s]+/giu,
      "***REDACTED_URI***",
    )
    .replace(/Bearer\s+\S+/giu, "Bearer ***REDACTED***")
    .replace(
      /(["'][^"'\r\n]*(?:api[_-]?key|access[_-]?key|secret|token|password|authorization|credential|private[_-]?key)[^"'\r\n]*["']\s*:\s*)(?:"[^"\r\n]*"|'[^'\r\n]*'|[^,\s}\]]+)/giu,
      '$1"***REDACTED***"',
    )
    .replace(
      /\b((?=[a-z0-9_-]*(?:api[_-]?key|access[_-]?key|secret|token|password|authorization|credential|private[_-]?key))[a-z][a-z0-9_-]*\s*[:=]\s*)(?:"[^"\r\n]*"|'[^'\r\n]*'|[^\s,;]+)/giu,
      "$1***REDACTED***",
    )
    .replace(
      /([?&](?:token|secret|signature|key)=)[^&\s]+/giu,
      "$1***REDACTED***",
    );
}

async function readBoundedBody(response: Response): Promise<Uint8Array> {
  if (!response.body) {
    throw new PaperclipAdapterDispositionError(
      "HTTP adapter disposition response body is missing.",
    );
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let total = 0;
  try {
    while (true) {
      const next = await reader.read();
      if (next.done) break;
      total += next.value.byteLength;
      if (total > HTTP_ADAPTER_DISPOSITION_MAX_BYTES) {
        await reader.cancel();
        throw new PaperclipAdapterDispositionError(
          "HTTP adapter disposition response exceeds the size limit.",
        );
      }
      chunks.push(next.value);
    }
  } finally {
    reader.releaseLock();
  }
  const combined = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    combined.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return combined;
}
