const DEFAULT_TOKEN = 'otc';
const CF_FALLBACK_IPS = ['[2a00:1098:2b::1:6815:5881]'];
const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;
const encoder = new TextEncoder();

import { connect } from 'cloudflare:sockets';

export default {
  async fetch(request, env, ctx) {
    try {
      const AUTH_TOKEN = env.TOKEN ?? DEFAULT_TOKEN;
      const upgradeHeader = request.headers.get('Upgrade');

      if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
        const { pathname } = new URL(request.url);
        return pathname === '/'
          ? new Response('WebSocket Proxy Server', { status: 200 })
          : new Response('Expected WebSocket', { status: 426 });
      }

      if (AUTH_TOKEN && request.headers.get('Sec-WebSocket-Protocol') !== AUTH_TOKEN) {
        return new Response('Unauthorized', { status: 401 });
      }

      const [client, server] = Object.values(new WebSocketPair());
      server.accept();

      ctx.waitUntil(handleSession(server));

      const responseInit = { status: 101, webSocket: client };
      if (AUTH_TOKEN) {
        responseInit.headers = { 'Sec-WebSocket-Protocol': AUTH_TOKEN };
      }

      return new Response(null, responseInit);
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  },
};

async function handleSession(webSocket) {
  let remoteSocket, remoteWriter, remoteReader;
  let isClosed = false;

  const cleanup = () => {
    if (isClosed) return;
    isClosed = true;

    try { remoteReader?.cancel(); }      catch {}
    try { remoteWriter?.releaseLock(); } catch {}
    try { remoteWriter?.close(); }       catch {}
    try { remoteSocket?.close(); }       catch {}

    remoteReader = remoteWriter = remoteSocket = null;
    safeCloseWebSocket(webSocket);
  };

  const pumpRemoteToWebSocket = async () => {
    try {
      while (!isClosed && remoteReader) {
        const { done, value } = await remoteReader.read();
        if (done || isClosed) break;
        if (webSocket.readyState !== WS_READY_STATE_OPEN) break;
        if (value?.byteLength > 0) webSocket.send(value);
      }
    } catch {}

    if (!isClosed) {
      try { webSocket.send('CLOSE'); } catch {}
      cleanup();
    }
  };

  const isCFError = (err) => {
    const msg = err?.message?.toLowerCase() ?? '';
    return msg.includes('proxy request') ||
           msg.includes('cannot connect') ||
           msg.includes('cloudflare');
  };

  const connectToRemote = async (targetAddr, firstFrameData) => {
    const { host, port } = parseAddress(targetAddr);
    const attempts = [null, ...CF_FALLBACK_IPS];

    for (let i = 0; i < attempts.length; i++) {
      try {
        remoteSocket = connect({ hostname: attempts[i] ?? host, port });
        await remoteSocket.opened;

        remoteWriter = remoteSocket.writable.getWriter();
        remoteReader = remoteSocket.readable.getReader();

        if (firstFrameData) {
          await remoteWriter.write(encoder.encode(firstFrameData));
        }

        webSocket.send('CONNECTED');
        pumpRemoteToWebSocket();
        return;
      } catch (err) {
        try { remoteReader?.cancel(); }      catch {}
        try { remoteWriter?.releaseLock(); } catch {}
        try { remoteSocket?.close(); }       catch {}
        remoteReader = remoteWriter = remoteSocket = null;

        if (!isCFError(err) || i === attempts.length - 1) {
          cleanup();
          return;
        }
      }
    }
  };

  webSocket.addEventListener('message', async ({ data }) => {
    if (isClosed) return;
    try {
      if (typeof data === 'string') {
        if (data.startsWith('CONNECT:')) {
          const sep = data.indexOf('|', 8);
          if (sep !== -1) {
            await connectToRemote(data.substring(8, sep), data.substring(sep + 1));
          }
        } else if (data.startsWith('DATA:')) {
          if (remoteWriter) await remoteWriter.write(encoder.encode(data.substring(5)));
        } else if (data === 'CLOSE') {
          cleanup();
        }
      } else if (data instanceof ArrayBuffer && remoteWriter) {
        await remoteWriter.write(new Uint8Array(data));
      }
    } catch (err) {
      try { webSocket.send('ERROR:' + err.message); } catch {}
      cleanup();
    }
  });

  webSocket.addEventListener('close', cleanup);
  webSocket.addEventListener('error', cleanup);
}

function parseAddress(addr) {
  if (addr.startsWith('[')) {
    const end = addr.indexOf(']');
    return { host: addr.substring(1, end), port: parseInt(addr.substring(end + 2), 10) };
  }
  const sep = addr.lastIndexOf(':');
  return { host: addr.substring(0, sep), port: parseInt(addr.substring(sep + 1), 10) };
}

function safeCloseWebSocket(ws) {
  try {
    if (ws.readyState === WS_READY_STATE_OPEN ||
        ws.readyState === WS_READY_STATE_CLOSING) {
      ws.close(1000, 'Server closed');
    }
  } catch {}
}
