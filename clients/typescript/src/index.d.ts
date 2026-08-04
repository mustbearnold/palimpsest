export type JsonValue =
  null | boolean | number | string | JsonValue[] | { [key: string]: JsonValue };
export type JsonObject = Record<string, any>;

export function projectNamespace(projectId: string, prefix?: string): string;
export function compareProjectBundles(
  bundles: Record<string, JsonObject>,
): JsonObject;
export function validateProjectReview(
  comparisonResult: JsonObject,
  review: JsonObject,
): JsonObject;
export function prepareProjectConsolidation(
  validatedReview: JsonObject,
  writes: JsonObject[],
  options: { consolidationId: string },
): JsonObject;

export interface PalimpsestResponse<T extends JsonObject = JsonObject> {
  data: T;
  statusCode: number;
  headers: Record<string, string>;
  etag: string | null;
  location: string | null;
}

export interface PalimpsestBinaryResponse {
  content: Uint8Array;
  statusCode: number;
  headers: Record<string, string>;
  etag: string | null;
  location: string | null;
}

export class PalimpsestError extends Error {}
export class PalimpsestConfigurationError extends PalimpsestError {}
export class PalimpsestTransportError extends PalimpsestError {}
export class PalimpsestProtocolError extends PalimpsestError {}
export class PalimpsestTimeoutError extends PalimpsestError {}
export class PalimpsestHttpError extends PalimpsestError {
  readonly statusCode: number;
  readonly method: string;
  readonly path: string;
  readonly problem: unknown;
  readonly headers: Record<string, string>;
  constructor(
    statusCode: number,
    method: string,
    path: string,
    problem: unknown,
    headers: Record<string, string>,
  );
}
export class PartialRememberError extends PalimpsestError {
  readonly episode: JsonObject;
  readonly cause: PalimpsestError;
  constructor(episode: JsonObject, cause: PalimpsestError);
}
export class PartialConsolidationError extends PalimpsestError {
  readonly consolidationId: string;
  readonly completed: JsonObject[];
  readonly failedWrite: JsonObject;
  readonly cause: PalimpsestError;
  constructor(
    consolidationId: string,
    completed: JsonObject[],
    failedWrite: JsonObject,
    cause: PalimpsestError,
  );
}

export interface ClientOptions {
  baseUrl: string;
  bearerToken: string;
  tenantId: string;
  subjectId: string;
  caseId?: string | null;
  timeoutMs?: number;
}

export class PalimpsestClient {
  constructor(options: ClientOptions);
  appendEpisode(options: {
    kind: string;
    observedAt: string;
    provenance: JsonObject;
    sensitivity: string;
    retentionPolicyId: string;
    payload: JsonValue;
    caseId?: string | null;
    idempotencyKey?: string | null;
  }): Promise<JsonObject>;
  createFact(options: {
    namespace: string;
    key: string;
    value: JsonValue;
    observedAt: string;
    validTime: JsonObject;
    evidenceEpisodeIds: string[];
    writePolicy: JsonObject;
    confidence: number;
    sensitivity: string;
    retentionPolicyId: string;
    caseId?: string | null;
    idempotencyKey?: string | null;
  }): Promise<JsonObject>;
  getFact(factId: string): Promise<JsonObject>;
  getFactResponse(factId: string): Promise<PalimpsestResponse>;
  getFactAsOf(
    factId: string,
    options: { validAt: string; recordedAt: string },
  ): Promise<JsonObject>;
  supersedeFact(
    factId: string,
    options: JsonObject & { ifMatch: string },
  ): Promise<JsonObject>;
  retrieve(query: string, options?: JsonObject): Promise<JsonObject>;
  recall(query: string, options?: JsonObject): Promise<JsonObject>;
  recallByProject(
    query: string,
    projectIds: string[],
    options?: {
      perspective?: string | JsonObject;
      pageSize?: number;
      policyId?: string | null;
      filters?: JsonObject;
      namespacePrefix?: string;
      idempotencyKeyPrefix?: string | null;
    },
  ): Promise<Record<string, JsonObject>>;
  compareByProject(
    query: string,
    projectIds: string[],
    options?: {
      perspective?: string | JsonObject;
      pageSize?: number;
      policyId?: string | null;
      filters?: JsonObject;
      namespacePrefix?: string;
      idempotencyKeyPrefix?: string | null;
    },
  ): Promise<JsonObject>;
  consolidateProjectReview(
    comparisonResult: JsonObject,
    review: JsonObject,
    writes: JsonObject[],
    options: { consolidationId: string },
  ): Promise<JsonObject>;
  getRetrieval(
    retrievalId: string,
    options?: { cursor?: string | null },
  ): Promise<JsonObject>;
  saveCheckpointResponse(
    agentId: string,
    threadId: string,
    options: JsonObject,
  ): Promise<PalimpsestResponse>;
  saveCheckpoint(
    agentId: string,
    threadId: string,
    options: JsonObject,
  ): Promise<JsonObject>;
  getCheckpointResponse(
    agentId: string,
    threadId: string,
  ): Promise<PalimpsestResponse>;
  getCheckpoint(agentId: string, threadId: string): Promise<JsonObject>;
  startExportResponse(options?: {
    idempotencyKey?: string | null;
  }): Promise<PalimpsestResponse>;
  startExport(options?: {
    idempotencyKey?: string | null;
  }): Promise<JsonObject>;
  getExportResponse(
    exportId: string,
    options?: { ifNoneMatch?: string | null },
  ): Promise<PalimpsestResponse>;
  getExport(
    exportId: string,
    options?: { ifNoneMatch?: string | null },
  ): Promise<JsonObject>;
  downloadExportResponse(exportId: string): Promise<PalimpsestBinaryResponse>;
  downloadExport(exportId: string): Promise<Uint8Array>;
  forget(options?: { idempotencyKey?: string | null }): Promise<JsonObject>;
  deleteSubject(options?: {
    idempotencyKey?: string | null;
  }): Promise<JsonObject>;
  getDeletionResponse(
    operationId: string,
    options?: { ifNoneMatch?: string | null },
  ): Promise<PalimpsestResponse>;
  getDeletion(
    operationId: string,
    options?: { ifNoneMatch?: string | null },
  ): Promise<JsonObject>;
  waitForDeletion(
    operationId: string,
    options?: { timeoutMs?: number; pollIntervalMs?: number },
  ): Promise<JsonObject>;
  remember(
    content: string,
    options: JsonObject & { key: string },
  ): Promise<{ episode: JsonObject; fact: JsonObject }>;
  correct(
    factId: string,
    options: JsonObject & { ifMatch: string },
  ): Promise<JsonObject>;
}
