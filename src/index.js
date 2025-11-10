import { SERVICE_MAP } from './servicemap.js';


async function handleRest(request, env, config, internalPath, url) {
  const token = env[config.authKeyEnvName];
  const cleanedTargetUrl = config.targetUrl.endsWith('/')
    ? config.targetUrl.slice(0, -1)
    : config.targetUrl;

  const finalUrl = cleanedTargetUrl + internalPath + url.search;
  let newBody = request.body;

  try {
    const clone = request.clone();
    const text = await clone.text();

    if (text && text.trim().length > 0) {
      try {
        const data = JSON.parse(text);

        if (config.table_name) {
          data.table_name = config.table_name;
        } else if ("table_name" in data) {
          delete data.table_name;
        }

        newBody = JSON.stringify(data);
      } catch {
        console.warn(`Non-JSON or unparseable body for ${finalUrl}, skipping modification.`);
      }
    }
  } catch (err) {
    console.warn(`Failed to read request body for ${finalUrl}:`, err);
  }

  const requestToForward = new Request(finalUrl, {
    method: request.method,
    headers: new Headers(request.headers),
    body: newBody,
  });

  requestToForward.headers.set("Authorization", `Bearer ${token}`);

  const response = await fetch(requestToForward);

  try {
    const json = await response.json();
    return jsonSuccess(json);
  } catch {
    const text = await response.text();
    return new Response(text, {
      status: response.status,
      headers: { "Content-Type": response.headers.get("Content-Type") || "text/plain" },
    });
  }
}

async function handleAbly(request, env, config, internalPath) {
  const apiKey = env[config.authKeyEnvName];
  const channel = config.channelName;
  const publishEndpoint = `${ABLY_PUBLISH_BASE_URL}/${channel}/messages`;

  let clientPayload;
  try {
    clientPayload = await request.clone().json();
  } catch {
    return jsonError(`ABLY event body must be valid JSON for path: ${internalPath}`, 400);
  }

  const messageBusPayload = JSON.stringify([{
    name: internalPath.slice(1).replace(/\//g, '.'),
    data: clientPayload,
  }]);

  const publishRequest = new Request(publishEndpoint, {
    method: 'POST',
    headers: {
      'Authorization': `Basic ${base64Encode(apiKey + ':')}`,
      'Content-Type': 'application/json',
    },
    body: messageBusPayload,
  });

  const publishResponse = await fetch(publishRequest);

  if (publishResponse.ok) {
    return jsonSuccess({
      status: 'Accepted',
      message: `Event published successfully to Ably channel: ${channel}`,
    });
  } else {
    return jsonError(`Failed to publish event to Ably: ${await publishResponse.text()}`, 502);
  }
}

// --- D1 Route Lookup ---
async function findConfigFromDB(key) {
  if (!G_DB) return null;

  try {
    const query = `SELECT t1 FROM ${C_RouteTableName} WHERE c1 = ? LIMIT 1`;
    const { results } = await G_DB.prepare(query).bind(key).all();

    if (!results || results.length === 0) return null;

    const row = results[0];
    if (!row.t1) return null;

    try {
      return JSON.parse(row.t1);
    } catch (err) {
      console.error(`Invalid JSON in DB for route ${key}:`, err);
      return null;
    }
  } catch (e) {
    console.error("DB lookup failed for route:", key, e);
    return null;
  }
}

// --- Worker Entrypoint ---
export default {
  async fetch(request, env) {
    G_DB = env.DB;
    const masterToken = env[C_GATEWAY_TOKEN_NAME];
    const authHeader = request.headers.get("Authorization") || "";
    const token = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : null;

    if (token !== masterToken) {
      return jsonError("Unauthorized", 401);
    }

    const url = new URL(request.url);
    const path = url.pathname;
    const pathSegments = path.split('/').filter(Boolean);

    if (pathSegments.length < 2) {
      return jsonError('Invalid API Path Format. Expected /{version}/{module}/...', 400);
    }

    const lookupKey = `${pathSegments[0]}/${pathSegments[1]}`;

    let config = SERVICE_MAP[lookupKey] || await findConfigFromDB(lookupKey);
    if (!config) {
      return jsonError('Endpoint Not Found or Unsupported Version', 404);
    }

    if (config.authKeyEnvName && !env[config.authKeyEnvName]) {
      console.error(`Missing env var: ${config.authKeyEnvName} for ${lookupKey}`);
      return jsonError('Gateway configuration error: Missing Secret Key for Downstream Service', 500);
    }

    const internalPath = '/' + pathSegments.slice(1).join('/');

    try {
      switch (config.type) {
        case 'REST':
          return handleRest(request, env, config, internalPath, url);
        case 'ABLY':
          return handleAbly(request, env, config, internalPath);
        default:
          return jsonError(`Unsupported service type: ${config.type}`, 501);
      }
    } catch (e) {
      console.error(`Error processing ${config.type}:`, e);
      return jsonError('Gateway processing failed', 500);
    }
  },
};

// --- Utilities ---
function base64Encode(str) {
  return btoa(str);
}
function jsonError(msg, code = 400) {
  return new Response(JSON.stringify({ success: false, error: msg }), {
    status: code,
    headers: { "Content-Type": "application/json" },
  });
}
function jsonSuccess(data) {
  return new Response(JSON.stringify({ success: true, ...data }), {
    headers: { "Content-Type": "application/json" },
  });
}


let G_DB = null;
const C_RouteTableName = "darouter";
const ABLY_PUBLISH_BASE_URL = "https://rest.ably.io/channels";
const C_GATEWAY_TOKEN_NAME = "DAGATEWAYTOKEN";
