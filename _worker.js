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
  let isConnecting = false;
  let isClosed = false;

  const cleanup = () => {
    if (isClosed) return;
    isClosed = true;

    try { remoteReader?.cancel(); }      catch {}
    try { remoteWriter?.releaseLock(); } catch {}
    try { remoteSocket?.close(); }       catch {}

    remoteReader = remoteWriter = remoteSocket = null;
    safeCloseWebSocket(webSocket);
  };

  const pumpRemoteToWebSocket = async () => {
    let chunkBuf = new ArrayBuffer(65536);
    const reader = remoteReader;

    try {
      while (!isClosed) {
        const { done, value } = await reader.read(new Uint8Array(chunkBuf));
        if (done || isClosed) break;
        if (webSocket.readyState !== WS_READY_STATE_OPEN) break;
        chunkBuf = value.buffer;
        if (value.byteLength > 0) webSocket.send(value.slice());
      }
    } catch {}

    try { reader.releaseLock(); } catch {}
    remoteReader = null;

    if (!isClosed) {
      try { webSocket.send('CLOSE'); } catch {}
      cleanup();
    }
  };

  const connectToRemote = async (targetAddr, firstFrameData) => {
    if (isConnecting || remoteSocket) return;
    isConnecting = true;

    const { host, port } = parseAddress(targetAddr);
    const attempts = [host, ...CF_FALLBACK_IPS.map(ip => ip.slice(1, -1))];

    for (let i = 0; i < attempts.length; i++) {
      let socket, writer, reader;
      try {
        socket = connect({ hostname: attempts[i], port });
        await socket.opened;

        writer = socket.writable.getWriter();
        reader = socket.readable.getReader({ mode: 'byob' });

        if (firstFrameData) {
          await writer.write(encoder.encode(firstFrameData));
        }

        remoteSocket = socket;
        remoteWriter = writer;
        remoteReader = reader;

        webSocket.send('CONNECTED');
        isConnecting = false;
        pumpRemoteToWebSocket();
        return;
      } catch {
        try { reader?.cancel(); }      catch {}
        try { writer?.releaseLock(); } catch {}
        try { socket?.close(); }       catch {}

        if (i === attempts.length - 1) {
          isConnecting = false;
          cleanup();
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
