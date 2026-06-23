import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { ObservabilityService } from "./gen/wavespan/v1/observability_pb";
import { Cypher } from "./gen/wavespan/v1/cypher_pb";
import { CollectionService } from "./gen/wavespan/v1/collections_pb";

// Same-origin transport against the admin port (the SPA is served from it). The Cypher service is
// mounted on the admin port too, so the console runs without cross-origin. Credentials/admin token
// ride along with the browser's cookies/headers (design/26).
const transport = createConnectTransport({
  baseUrl: window.location.origin,
  fetch: (input, init) => fetch(input, { ...init, credentials: "same-origin" }),
});

export const obs = createClient(ObservabilityService, transport);
export const cypher = createClient(Cypher, transport);
export const collections = createClient(CollectionService, transport);
