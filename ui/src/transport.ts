import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { ObservabilityService } from "./gen/wavespan/v1/observability_pb";

// Same-origin transport against the admin port (the SPA is served from it). Credentials/admin token
// ride along with the browser's cookies/headers (design/26).
const transport = createConnectTransport({
  baseUrl: window.location.origin,
  fetch: (input, init) => fetch(input, { ...init, credentials: "same-origin" }),
});

export const obs = createClient(ObservabilityService, transport);
