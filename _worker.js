const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;
const CF_FALLBACK_IPS = ['[2a00:1098:2b::1:6815:5881]'];
const WS_BACKPRESSURE_LIMIT = 2 * 1024 * 1024;

const encoder = new TextEncoder();

import { connect } from 'cloudflare:sockets';

export default {
  async fetch(request, env, ctx) {
    try {
      const token = '';
      const upgradeHeader = request.headers.get('Upgrade');
      
      if (!upgradeHeader || upgradeHeader.toLowerCase() !== 'websocket') {
        return new URL(request.url).pathname === '/' 
          ? new Response('WebSocket Proxy Server', { status: 200 })
          : new Response('Expected WebSocket', { status: 426 });
      }

      if (token && request.headers.get('Sec-WebSocket-Protocol') !== token) {
        return new Response('Unauthorized', { status: 401 });
      }

      const [client, server] = Object.values(new WebSocketPair());
      server.accept();
      
      handleSession(server).catch(err => console.error('Session error:', err));

      const responseInit = { status: 101, webSocket: client };
      if (token) responseInit.headers = { 'Sec-WebSocket-Protocol': token };

      return new Response(null, responseInit);
      
    } catch (err) {
      return new Response(err.toString(), { status: 500 });
    }
  },
};

async function handleSession(webSocket) {
  let remoteSocket, remoteWriter, remoteReader;
  let isClosed = false;

  // pump 重入锁
  let isPumping = false;

  // 三态连接状态机：idle -> connecting -> connected
  // 防止 CONNECT 风暴：remoteSocket/isPumping 覆盖不到的 connecting 窗口
  let isConnecting = false;

  // writer 串行锁
  // - isClosed 前置短路，防止 cleanup 后继续入队
  // - writeEpoch 版本号：cleanup 时自增，使所有旧写入在 .then() 内逻辑层面失效
  // - writer 身份校验：防止 writer 被替换后写入旧引用
  let writing = Promise.resolve();
  let writeEpoch = 0;
  const safeWrite = (chunk) => {
    if (isClosed) return;
    const writer = remoteWriter;
    const epoch = writeEpoch;
    if (!writer) return;
    writing = writing.then(async () => {
      if (isClosed || epoch !== writeEpoch || writer !== remoteWriter) return;
      await writer.write(chunk);
    }).catch(() => {});
  };

  const cleanup = () => {
    if (isClosed) return;
    isClosed = true;
    writeEpoch++;         // 使所有旧写入队列在逻辑层面立即失效

    // 先发协议级操作（cancel/close），再释放 JS 流锁（releaseLock）
    try { remoteReader?.cancel(); } catch {}
    try { remoteWriter?.close(); } catch {}
    try { remoteReader?.releaseLock(); } catch {}
    try { remoteWriter?.releaseLock(); } catch {}
    try { if (remoteSocket) remoteSocket.close(); } catch {}
    
    remoteWriter = remoteReader = remoteSocket = null;
    writing = Promise.resolve();
    safeCloseWebSocket(webSocket);
  };

  const pumpRemoteToWebSocket = async () => {
    if (isPumping) return;
    isPumping = true;

    try {
      while (!isClosed && remoteReader) {
        const { done, value } = await remoteReader.read();
        
        if (done) break;
        if (webSocket.readyState !== WS_READY_STATE_OPEN) break;
        if (value?.byteLength > 0) {
          // 软背压：指数退避，加入 isClosed 提前退出防止关闭后空转
          let delay = 2;
          while (
            !isClosed &&
            webSocket.readyState === WS_READY_STATE_OPEN &&
            webSocket.bufferedAmount > WS_BACKPRESSURE_LIMIT
          ) {
            await new Promise(r => setTimeout(r, delay));
            delay = Math.min(delay * 1.5, 20);
          }
          if (!isClosed && webSocket.readyState === WS_READY_STATE_OPEN) {
            // 优先发送底层 ArrayBuffer，避免 Uint8Array 视图在部分 CF runtime 触发额外内存拷贝
            const payload =
              value.byteOffset === 0 && value.byteLength === value.buffer.byteLength
                ? value.buffer
                : value;
            webSocket.send(payload);
          }
        }
      }
    } catch {}
    finally {
      isPumping = false;
    }
    
    // 仅在连接确实开着时才发 CLOSE，避免双向重复帧
    if (!isClosed) {
      if (webSocket.readyState === WS_READY_STATE_OPEN) {
        try { webSocket.send('CLOSE'); } catch {}
      }
      cleanup();
    }
  };

  const parseAddress = (addr) => {
    if (addr[0] === '[') {
      const end = addr.indexOf(']', 1);   // 从 1 起扫，跳过已知的 '['
      return { host: addr.slice(1, end), port: +addr.slice(end + 2) };
    }
    const sep = addr.lastIndexOf(':');
    return { host: addr.slice(0, sep), port: +addr.slice(sep + 1) };
  };

  const isCFError = (err) => {
    const msg = err?.message?.toLowerCase() || '';
    return msg.includes('proxy request') || 
           msg.includes('cannot connect') || 
           msg.includes('cloudflare');
  };

  const connectToRemote = async (targetAddr, firstFrameData) => {
    isConnecting = true;
    try {
      const { host, port } = parseAddress(targetAddr);
      const attempts = [null, ...CF_FALLBACK_IPS]; 

      for (let i = 0; i < attempts.length; i++) {
        const currentHostname = attempts[i] || host;
        
        try {
          remoteSocket = connect({ hostname: currentHostname, port });

          // 超时保护：finally 确保无论 resolve/reject 都清理 timer，消除残留
          if (remoteSocket.opened) {
            let timeoutId;
            try {
              await Promise.race([
                remoteSocket.opened,
                new Promise((_, reject) => {
                  timeoutId = setTimeout(() => reject(new Error('Connect timeout')), 8000);
                })
              ]);
            } finally {
              clearTimeout(timeoutId);
            }
          }

          remoteWriter = remoteSocket.writable.getWriter();
          remoteReader = remoteSocket.readable.getReader();

          if (firstFrameData) {
            await remoteWriter.write(encoder.encode(firstFrameData));
          }

          webSocket.send('CONNECTED');
          pumpRemoteToWebSocket();
          return;

        } catch (err) {
          try { remoteWriter?.releaseLock(); } catch {}
          try { remoteReader?.releaseLock(); } catch {}
          try { remoteSocket?.close(); } catch {}
          remoteWriter = remoteReader = remoteSocket = null;

          if (i < attempts.length - 1) {
            if (!isCFError(err) && i === 0) throw err;
            continue; 
          }
          throw err;
        }
      }
    } finally {
      isConnecting = false;
    }
  };

  webSocket.addEventListener('message', async (event) => {
    if (isClosed) return;

    try {
      const data = event.data;

      if (typeof data === 'string') {
        if (data.startsWith('CONNECT:')) {
          // 三态保护：connecting 窗口 + 已连接 + pump 运行中 均拒绝重入
          if (isConnecting || remoteSocket || isPumping) {
            throw new Error('Already connected');
          }
          const sep = data.indexOf('|', 8);
          await connectToRemote(
            data.substring(8, sep),
            data.substring(sep + 1)
          );
        }
        else if (data.startsWith('DATA:')) {
          if (remoteWriter) {
            safeWrite(encoder.encode(data.substring(5)));
          }
        }
        else if (data === 'CLOSE') {
          if (remoteSocket) {
            try {
              if (remoteWriter) await remoteWriter.close();
              if (remoteReader) await remoteReader.cancel();
              await remoteSocket.close();
            } catch {}
          }
          cleanup();
          return;
        }
      }
      // byteLength 检测替代 instanceof，兼容 Worker 环境下的 Uint8Array binaryType
      else if (remoteWriter && data?.byteLength !== undefined) {
        safeWrite(new Uint8Array(data));
      }
    } catch (err) {
      try { webSocket.send('ERROR:' + err.message); } catch {}
      cleanup();
    }
  });

  webSocket.addEventListener('close', cleanup);
  webSocket.addEventListener('error', cleanup);
}

function safeCloseWebSocket(ws) {
  try {
    if (ws.readyState === WS_READY_STATE_OPEN || 
        ws.readyState === WS_READY_STATE_CLOSING) {
      ws.close(1000, 'Server closed');
    }
  } catch {}
}
