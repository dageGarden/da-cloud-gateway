import { SERVICE_MAP } from './servicemap.js';

/*
  ==========================================================
   DaSystem Gateway Service (v2 / unified API)
  ==========================================================
   Endpoint: POST /api
   Description:
     - Accepts DaSystem standard request format
     - Combines version + service to build routing key
     - Checks static service_map first, then optional DB router
     - Returns ack/nack exactly like the Log Service
*/

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (request.method !== "POST") {
      return new Response("Method Not Allowed", { status: 405 });
    }

    if (url.pathname === "/api") {
      return await handleApi(request, env);
    } else {
      return new Response("Not Found", { status: 404 });
    }
  },
};

/////////////////////////   Main Handler   /////////////////////////
async function handleApi(request, env) {
  // 🔐 Auth check
  const auth = request.headers.get("Authorization");
  if (!auth || !auth.startsWith("Bearer ")) {
    return nack("unknown", "UNAUTHORIZED", "Missing or invalid Authorization header");
  }

  const token = auth.split(" ")[1];
  if (token !== env.GATEWAY_MASTER_TOKEN) {
    return nack("unknown", "INVALID_TOKEN", "Token authentication failed");
  }

  // 🧩 Parse JSON
  let body;
  try {
    body = await request.json();
  } catch {
    return nack("unknown", "INVALID_JSON", "Malformed JSON body");
  }

  const requestId = body.request_id || "unknown";
  const { version, service, action, payload } = body;

  // 🧪 Validate required fields
  if (!version || !service || !action || !payload) {
    return nack(requestId, "INVALID_FIELD", "Missing one of: version, service, action, payload");
  }

  const key = `${version}.${service}`;
  let targetUrl = SERVICE_MAP[key];

  // 🗄️ Optional: check router DB table if not found in static map
  if (!targetUrl && env.DB) {
    try {
      const row = await env.DB.prepare("SELECT url FROM router WHERE route_key = ?")
        .bind(key)
        .first();
      if (row && row.url) targetUrl = row.url;
    } catch (err) {
      await logError(`DB router lookup failed for ${key}: ${err.message}`);
    }
  }

  if (!targetUrl) {
    return nack(requestId, "SERVICE_NOT_FOUND", `No route found for key: ${key}`);
  }

  try {
    // 🔁 Forward request
    const res = await fetch(targetUrl, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${env.GATEWAY_FORWARD_TOKEN || token}`,
      },
      body: JSON.stringify(body),
    });

    const text = await res.text();
    let json;
    try {
      json = JSON.parse(text);
    } catch {
      return nack(requestId, "INVALID_SERVICE_RESPONSE", "Service did not return valid JSON");
    }

    return jsonResponse(json, res.status);
  } catch (err) {
    await logError(`Forward error to ${targetUrl}: ${err.message}`);
    return nack(requestId, "FORWARD_ERROR", err.message);
  }
}

/////////////////////////   Static Service Map   /////////////////////////
const SERVICE_MAP = {
  "v1.log": "https://logservice.workers.dev/api",
  "v1.config": "https://configservice.workers.dev/api",
  "v1.storage": "https://storageservice.workers.dev/api",
  // Add other frequently used internal cloud services here
};

/////////////////////////   Utility   /////////////////////////
function ack(requestId) {
  return jsonResponse({ type: "ack", request_id: requestId });
}

function nack(requestId, code, message) {
  return jsonResponse(
    {
      type: "nack",
      request_id: requestId,
      payload: { status: "error", code, message },
    },
    400
  );
}

function jsonResponse(obj, status = 200) {
  return new Response(JSON.stringify(obj, null, 2), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

async function logError(msg) {
  console.error(`❌ ${msg}`);
}


let G_DB = null;
const C_RouteTableName = "darouter";
const ABLY_PUBLISH_BASE_URL = "https://rest.ably.io/channels";
const C_GATEWAY_TOKEN_NAME = "DAGATEWAYTOKEN";
