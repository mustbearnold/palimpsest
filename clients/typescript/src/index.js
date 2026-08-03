import { randomUUID } from "node:crypto";

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;

export class PalimpsestError extends Error {}

export class PalimpsestConfigurationError extends PalimpsestError {}

export class PalimpsestTransportError extends PalimpsestError {}

export class PalimpsestProtocolError extends PalimpsestError {}

export class PalimpsestTimeoutError extends PalimpsestError {}

export class PalimpsestHttpError extends PalimpsestError {
  constructor(statusCode, method, path, problem, headers) {
    const type = problem && typeof problem === "object" && typeof problem.type === "string"
      ? ` (${problem.type})`
      : "";
    super(`Palimpsest returned HTTP ${statusCode}${type} for ${method} ${path}`);
    this.name = "PalimpsestHttpError";
    this.statusCode = statusCode;
    this.method = method;
    this.path = path;
    this.problem = problem;
    this.headers = headers;
  }
}

export class PartialRememberError extends PalimpsestError {
  constructor(episode, cause) {
    const episodeId = episode && typeof episode === "object" ? episode.episode_id ?? "unknown" : "unknown";
    super(`episode ${episodeId} was saved, but fact promotion failed: ${cause.message}`);
    this.name = "PartialRememberError";
    this.episode = episode;
    this.cause = cause;
  }
}

export class PalimpsestClient {
  constructor({ baseUrl, bearerToken, tenantId, subjectId, caseId = null, timeoutMs = 30_000 }) {
    this.baseUrl = baseUrlValue(baseUrl);
    this.bearerToken = nonEmptyText(bearerToken, "bearerToken");
    this.tenantId = uuidValue(tenantId, "tenantId");
    this.subjectId = uuidValue(subjectId, "subjectId");
    this.caseId = caseId === null ? null : uuidValue(caseId, "caseId");
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
      throw new PalimpsestConfigurationError("timeoutMs must be greater than zero");
    }
    this.timeoutMs = timeoutMs;
  }

  async appendEpisode({
    kind,
    observedAt,
    provenance,
    sensitivity,
    retentionPolicyId,
    payload,
    caseId = null,
    idempotencyKey = null,
  }) {
    return this.#jsonRequest("POST", `${this.#scopePath()}/episodes`, {
      body: {
        case_id: this.#case(caseId),
        kind: nonEmptyText(kind, "kind"),
        observed_at: nonEmptyText(observedAt, "observedAt"),
        provenance: { ...provenance },
        sensitivity: nonEmptyText(sensitivity, "sensitivity"),
        retention_policy_id: nonEmptyText(retentionPolicyId, "retentionPolicyId"),
        payload,
      },
      idempotencyKey: idempotencyKeyValue(idempotencyKey),
    });
  }

  async createFact({
    namespace,
    key,
    value,
    observedAt,
    validTime,
    evidenceEpisodeIds,
    writePolicy,
    confidence,
    sensitivity,
    retentionPolicyId,
    caseId = null,
    idempotencyKey = null,
  }) {
    return this.#jsonRequest("POST", `${this.#scopePath()}/facts`, {
      body: {
        case_id: this.#case(caseId),
        namespace: nonEmptyText(namespace, "namespace"),
        key: nonEmptyText(key, "key"),
        value: nonNull(value, "value"),
        observed_at: nonEmptyText(observedAt, "observedAt"),
        valid_time: { ...validTime },
        evidence_episode_ids: evidenceEpisodeIds.map((id) => uuidValue(id, "evidenceEpisodeId")),
        write_policy: { ...writePolicy },
        confidence: confidenceValue(confidence),
        sensitivity: nonEmptyText(sensitivity, "sensitivity"),
        retention_policy_id: nonEmptyText(retentionPolicyId, "retentionPolicyId"),
      },
      idempotencyKey: idempotencyKeyValue(idempotencyKey),
    });
  }

  async getFact(factId) {
    return (await this.getFactResponse(factId)).data;
  }

  async getFactResponse(factId) {
    return this.#jsonResponse("GET", `${this.#scopePath()}/facts/${uuidValue(factId, "factId")}`);
  }

  async getFactAsOf(factId, { validAt, recordedAt }) {
    const query = new URLSearchParams({
      valid_at: nonEmptyText(validAt, "validAt"),
      recorded_at: nonEmptyText(recordedAt, "recordedAt"),
    });
    return this.#jsonRequest("GET", `${this.#scopePath()}/facts/${uuidValue(factId, "factId")}/as-of?${query}`);
  }

  async supersedeFact(factId, {
    supersedesRevisionId,
    value,
    observedAt,
    validTime,
    evidenceEpisodeIds,
    writePolicy,
    confidence,
    sensitivity,
    retentionPolicyId,
    ifMatch,
    idempotencyKey = null,
  }) {
    return this.#jsonRequest("PUT", `${this.#scopePath()}/facts/${uuidValue(factId, "factId")}`, {
      body: {
        supersedes_revision_id: uuidValue(supersedesRevisionId, "supersedesRevisionId"),
        value: nonNull(value, "value"),
        observed_at: nonEmptyText(observedAt, "observedAt"),
        valid_time: { ...validTime },
        evidence_episode_ids: evidenceEpisodeIds.map((id) => uuidValue(id, "evidenceEpisodeId")),
        write_policy: { ...writePolicy },
        confidence: confidenceValue(confidence),
        sensitivity: nonEmptyText(sensitivity, "sensitivity"),
        retention_policy_id: nonEmptyText(retentionPolicyId, "retentionPolicyId"),
      },
      idempotencyKey: idempotencyKeyValue(idempotencyKey),
      ifMatch: nonEmptyText(ifMatch, "ifMatch"),
    });
  }

  async retrieve(query, {
    perspective = "current",
    pageSize = 10,
    policyId = null,
    filters = {},
    idempotencyKey = null,
  } = {}) {
    const text = nonEmptyText(query, "query");
    if (new TextEncoder().encode(text).length > 4096) {
      throw new PalimpsestConfigurationError("query must contain at most 4096 UTF-8 bytes");
    }
    if (!Number.isInteger(pageSize) || pageSize < 1 || pageSize > 50) {
      throw new PalimpsestConfigurationError("pageSize must be an integer from 1 to 50");
    }
    const normalizedPerspective = perspective === "current"
      ? { kind: "current" }
      : perspective && typeof perspective === "object"
        ? { ...perspective }
        : (() => {
            throw new PalimpsestConfigurationError("perspective must be 'current' or an object");
          })();
    const body = {
      query: text,
      perspective: normalizedPerspective,
      page_size: pageSize,
      filters: { ...filters },
    };
    if (policyId !== null) body.policy_id = nonEmptyText(policyId, "policyId");
    return this.#jsonRequest("POST", `${this.#scopePath()}/retrievals`, {
      body,
      idempotencyKey: idempotencyKeyValue(idempotencyKey),
    });
  }

  async recall(query, options = {}) {
    return this.retrieve(query, options);
  }

  async recallByProject(query, projectIds, {
    perspective = "current",
    pageSize = 10,
    policyId = null,
    filters = {},
    namespacePrefix = "agent_session",
    idempotencyKeyPrefix = null,
  } = {}) {
    if (!Array.isArray(projectIds) || projectIds.length === 0) {
      throw new PalimpsestConfigurationError("projectIds must be a non-empty array");
    }
    if (!filters || typeof filters !== "object" || Array.isArray(filters)) {
      throw new PalimpsestConfigurationError("filters must be an object");
    }
    if (Object.hasOwn(filters, "namespaces")) {
      throw new PalimpsestConfigurationError("recallByProject owns the namespaces filter");
    }
    const selected = [];
    const namespaces = new Map();
    for (const projectId of projectIds) {
      const normalized = nonEmptyText(projectId, "projectId");
      if (namespaces.has(normalized)) continue;
      selected.push(normalized);
      namespaces.set(normalized, projectNamespace(normalized, namespacePrefix));
    }
    const baseKey = idempotencyKeyPrefix === null ? null : idempotencyBase(idempotencyKeyPrefix);
    const results = Object.create(null);
    for (const projectId of selected) {
      const idempotencyKey = baseKey === null ? null : `${baseKey}:${projectId}`;
      if (idempotencyKey !== null && idempotencyKey.length > 255) {
        throw new PalimpsestConfigurationError("idempotencyKeyPrefix leaves insufficient room for project IDs");
      }
      results[projectId] = await this.retrieve(query, {
        perspective,
        pageSize,
        policyId,
        filters: { ...filters, namespaces: [namespaces.get(projectId)] },
        idempotencyKey,
      });
    }
    return results;
  }

  async getRetrieval(retrievalId, { cursor = null } = {}) {
    const path = `${this.#scopePath()}/retrievals/${uuidValue(retrievalId, "retrievalId")}`;
    const suffix = cursor === null ? "" : `?${new URLSearchParams({ cursor: nonEmptyText(cursor, "cursor") })}`;
    return this.#jsonRequest("GET", `${path}${suffix}`);
  }

  async saveCheckpointResponse(agentId, threadId, {
    state,
    stateSchemaVersion,
    effectTransitions,
    provenance,
    sensitivity,
    retentionPolicyId,
    caseId = null,
    parentRevisionId = null,
    ifMatch = null,
    ifNoneMatch = null,
    idempotencyKey = null,
  }) {
    if ((ifMatch === null) === (ifNoneMatch === null)) {
      throw new PalimpsestConfigurationError("supply exactly one of ifMatch or ifNoneMatch");
    }
    if (ifNoneMatch !== null && ifNoneMatch !== "*") {
      throw new PalimpsestConfigurationError("ifNoneMatch must be '*'");
    }
    if (!Number.isInteger(stateSchemaVersion) || stateSchemaVersion < 1) {
      throw new PalimpsestConfigurationError("stateSchemaVersion must be a positive integer");
    }
    return this.#jsonResponse("PUT", this.#checkpointPath(agentId, threadId), {
      body: {
        case_id: this.#case(caseId),
        parent_revision_id: parentRevisionId === null ? null : uuidValue(parentRevisionId, "parentRevisionId"),
        state,
        state_schema_version: stateSchemaVersion,
        effect_transitions: effectTransitions.map((effect) => ({ ...effect })),
        provenance: { ...provenance },
        sensitivity: nonEmptyText(sensitivity, "sensitivity"),
        retention_policy_id: nonEmptyText(retentionPolicyId, "retentionPolicyId"),
      },
      idempotencyKey: idempotencyKeyValue(idempotencyKey),
      ifMatch,
      ifNoneMatch,
    });
  }

  async saveCheckpoint(agentId, threadId, options) {
    return (await this.saveCheckpointResponse(agentId, threadId, options)).data;
  }

  async getCheckpointResponse(agentId, threadId) {
    return this.#jsonResponse("GET", this.#checkpointPath(agentId, threadId));
  }

  async getCheckpoint(agentId, threadId) {
    return (await this.getCheckpointResponse(agentId, threadId)).data;
  }

  async startExportResponse({ idempotencyKey = null } = {}) {
    return this.#jsonResponse("POST", `${this.#scopePath()}/exports`, {
      idempotencyKey: idempotencyKeyValue(idempotencyKey),
    });
  }

  async startExport(options = {}) {
    return (await this.startExportResponse(options)).data;
  }

  async getExportResponse(exportId, { ifNoneMatch = null } = {}) {
    return this.#jsonResponse("GET", `${this.#scopePath()}/exports/${uuidValue(exportId, "exportId")}`, {
      ifNoneMatch,
    });
  }

  async getExport(exportId, options = {}) {
    return (await this.getExportResponse(exportId, options)).data;
  }

  async downloadExportResponse(exportId) {
    const response = await this.#request("GET", `${this.#scopePath()}/exports/${uuidValue(exportId, "exportId")}/content`);
    return {
      content: new Uint8Array(response.body),
      statusCode: response.statusCode,
      headers: response.headers,
      etag: response.headers.etag ?? null,
      location: response.headers.location ?? null,
    };
  }

  async downloadExport(exportId) {
    return (await this.downloadExportResponse(exportId)).content;
  }

  async forget({ idempotencyKey = null } = {}) {
    return this.#jsonRequest("POST", `${this.#scopePath()}/deletions`, {
      body: {},
      idempotencyKey: idempotencyKeyValue(idempotencyKey),
    });
  }

  async deleteSubject(options = {}) {
    return this.forget(options);
  }

  async getDeletionResponse(operationId, { ifNoneMatch = null } = {}) {
    return this.#jsonResponse("GET", `${this.#scopePath()}/deletions/${uuidValue(operationId, "operationId")}`, {
      ifNoneMatch,
    });
  }

  async getDeletion(operationId, options = {}) {
    return (await this.getDeletionResponse(operationId, options)).data;
  }

  async waitForDeletion(operationId, { timeoutMs = 30_000, pollIntervalMs = 500 } = {}) {
    if (!Number.isFinite(timeoutMs) || timeoutMs <= 0 || !Number.isFinite(pollIntervalMs) || pollIntervalMs <= 0) {
      throw new PalimpsestConfigurationError("timeoutMs and pollIntervalMs must be greater than zero");
    }
    const deadline = Date.now() + timeoutMs;
    let etag = null;
    let latest = null;
    while (true) {
      const response = await this.getDeletionResponse(operationId, { ifNoneMatch: etag });
      if (response.statusCode !== 304) {
        latest = response.data;
        etag = response.etag;
      }
      if (latest && ["completed", "failed", "expired"].includes(latest.lifecycle_state)) return latest;
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        throw new PalimpsestTimeoutError(`deletion ${operationId} did not reach a terminal state within ${timeoutMs} ms`);
      }
      await new Promise((resolve) => setTimeout(resolve, Math.min(pollIntervalMs, remaining)));
    }
  }

  async remember(content, {
    key,
    metadata = {},
    kind = "typescript_memory",
    sourceType = "palimpsest.typescript",
    sourceUri = null,
    externalId = null,
    namespace = "typescript",
    sensitivity = "internal",
    retentionPolicyId = "standard",
    confidence = 1,
    observedAt = new Date().toISOString(),
    idempotencyKey = null,
  }) {
    const text = nonEmptyText(content, "content");
    if (new TextEncoder().encode(text).length > 65_536) {
      throw new PalimpsestConfigurationError("content must contain at most 65536 UTF-8 bytes");
    }
    const memoryKey = nonEmptyText(key, "key");
    const baseKey = idempotencyBase(idempotencyKey);
    const episodePayload = { content: text, metadata: { ...metadata } };
    const episode = await this.appendEpisode({
      kind,
      observedAt,
      provenance: { source_type: nonEmptyText(sourceType, "sourceType"), source_uri: sourceUri, external_id: externalId },
      sensitivity,
      retentionPolicyId,
      payload: episodePayload,
      idempotencyKey: `${baseKey}:episode`,
    });
    if (!episode || typeof episode.episode_id !== "string" || !episode.episode_id) {
      throw new PalimpsestProtocolError("Palimpsest created an episode without returning its identifier");
    }
    try {
      const fact = await this.createFact({
        namespace,
        key: memoryKey,
        value: episodePayload,
        observedAt,
        validTime: { from: observedAt },
        evidenceEpisodeIds: [episode.episode_id],
        writePolicy: { id: "direct-evidence", version: "1" },
        confidence,
        sensitivity,
        retentionPolicyId,
        idempotencyKey: `${baseKey}:fact`,
      });
      return { episode, fact };
    } catch (error) {
      if (error instanceof PalimpsestError) throw new PartialRememberError(episode, error);
      throw error;
    }
  }

  async correct(factId, options) {
    return this.supersedeFact(factId, options);
  }

  #case(caseId) {
    const selected = caseId === null ? this.caseId : uuidValue(caseId, "caseId");
    if (selected === null) throw new PalimpsestConfigurationError("caseId is required for this operation");
    return selected;
  }

  #scopePath() {
    return `/v1/tenants/${encodeURIComponent(this.tenantId)}/subjects/${encodeURIComponent(this.subjectId)}`;
  }

  #checkpointPath(agentId, threadId) {
    return `${this.#scopePath()}/agents/${uuidValue(agentId, "agentId")}/threads/${uuidValue(threadId, "threadId")}/checkpoint`;
  }

  async #jsonRequest(method, path, options = {}) {
    return (await this.#jsonResponse(method, path, options)).data;
  }

  async #jsonResponse(method, path, options = {}) {
    const response = await this.#request(method, path, options);
    if (response.statusCode === 303 || response.statusCode === 304) {
      return responseEnvelope({}, response);
    }
    let decoded;
    try {
      decoded = JSON.parse(new TextDecoder().decode(response.body));
    } catch (error) {
      throw new PalimpsestProtocolError("Palimpsest returned invalid JSON", { cause: error });
    }
    if (!decoded || typeof decoded !== "object" || Array.isArray(decoded)) {
      throw new PalimpsestProtocolError("Palimpsest returned a non-object JSON response");
    }
    return responseEnvelope(decoded, response);
  }

  async #request(method, path, { body = undefined, idempotencyKey = null, ifMatch = null, ifNoneMatch = null } = {}) {
    const headers = {
      Accept: "application/json",
      Authorization: `Bearer ${this.bearerToken}`,
    };
    let encodedBody;
    if (body !== undefined) {
      headers["Content-Type"] = "application/json";
      encodedBody = JSON.stringify(body);
    }
    if (idempotencyKey !== null) headers["Idempotency-Key"] = idempotencyKeyValue(idempotencyKey);
    if (ifMatch !== null) headers["If-Match"] = ifMatch;
    if (ifNoneMatch !== null) headers["If-None-Match"] = ifNoneMatch;
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), this.timeoutMs);
    let response;
    try {
      response = await fetch(`${this.baseUrl}${path}`, {
        method,
        headers,
        body: encodedBody,
        redirect: "manual",
        signal: controller.signal,
      });
    } catch (error) {
      if (error?.name === "AbortError") {
        throw new PalimpsestTimeoutError(`Palimpsest request exceeded ${this.timeoutMs} ms`);
      }
      throw new PalimpsestTransportError(`Palimpsest is unavailable: ${error.message}`);
    } finally {
      clearTimeout(timeout);
    }
    const responseHeaders = Object.fromEntries(response.headers.entries());
    const bodyBytes = new Uint8Array(await response.arrayBuffer());
    const raw = { statusCode: response.status, headers: responseHeaders, body: bodyBytes };
    if (!response.ok && response.status !== 303 && response.status !== 304) {
      let problem = null;
      try {
        problem = JSON.parse(new TextDecoder().decode(bodyBytes));
      } catch {
        // Preserve the typed HTTP error even when a proxy returns non-JSON.
      }
      throw new PalimpsestHttpError(response.status, method, path, problem, responseHeaders);
    }
    return raw;
  }
}

function responseEnvelope(data, response) {
  return {
    data,
    statusCode: response.statusCode,
    headers: response.headers,
    etag: response.headers.etag ?? null,
    location: response.headers.location ?? null,
  };
}

function baseUrlValue(value) {
  if (typeof value !== "string") throw new PalimpsestConfigurationError("baseUrl must be a string");
  let parsed;
  try {
    parsed = new URL(value);
  } catch {
    throw new PalimpsestConfigurationError("baseUrl must be an HTTP(S) URL");
  }
  if (!["http:", "https:"].includes(parsed.protocol) || !parsed.host) {
    throw new PalimpsestConfigurationError("baseUrl must be an HTTP(S) URL");
  }
  if (parsed.search || parsed.hash || parsed.username || parsed.password) {
    throw new PalimpsestConfigurationError("baseUrl must not contain credentials, a query, or a fragment");
  }
  return value.replace(/\/+$/, "");
}

function uuidValue(value, name) {
  if (typeof value !== "string" || !UUID_PATTERN.test(value)) {
    throw new PalimpsestConfigurationError(`${name} must be a UUID string`);
  }
  return value.toLowerCase();
}

function nonEmptyText(value, name) {
  if (typeof value !== "string" || !value.trim()) {
    throw new PalimpsestConfigurationError(`${name} must be a non-empty string`);
  }
  return value.trim();
}

function nonNull(value, name) {
  if (value === null || value === undefined) throw new PalimpsestConfigurationError(`${name} must not be null`);
  return value;
}

function confidenceValue(value) {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0 || value > 1) {
    throw new PalimpsestConfigurationError("confidence must be a number from 0 to 1");
  }
  return value;
}

function idempotencyKeyValue(value) {
  if (value === null) return `palimpsest-typescript-${randomUUID()}`;
  if (typeof value !== "string" || !value.trim() || value.length > 255) {
    throw new PalimpsestConfigurationError("idempotencyKey must contain 1 to 255 characters");
  }
  return value;
}

function idempotencyBase(value) {
  const base = value === null ? `palimpsest-typescript-${randomUUID()}` : idempotencyKeyValue(value);
  if (base.length > 243) {
    throw new PalimpsestConfigurationError("idempotencyKey must leave room for the operation suffix");
  }
  return base;
}

export function projectNamespace(projectId, prefix = "agent_session") {
  const normalizedProject = nonEmptyText(projectId, "projectId");
  const normalizedPrefix = nonEmptyText(prefix, "namespacePrefix");
  const namespace = `${normalizedPrefix}:${normalizedProject}`;
  if (namespace.length > 255) {
    throw new PalimpsestConfigurationError("project namespace must contain at most 255 characters");
  }
  return namespace;
}
