const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const { EventEmitter } = require('events');

// 日志记录器模块
class LoggingService {
  constructor(serviceName = 'ProxyServer') {
    this.serviceName = serviceName;
  }

  _formatMessage(level, message) {
    const timestamp = new Date().toISOString();
    return `[${level}] ${timestamp} [${this.serviceName}] - ${message}`;
  }

  info(message) {
    console.log(this._formatMessage('INFO', message));
  }

  error(message) {
    console.error(this._formatMessage('ERROR', message));
  }

  warn(message) {
    console.warn(this._formatMessage('WARN', message));
  }

  debug(message) {
    console.debug(this._formatMessage('DEBUG', message));
  }
}

// 消息队列实现
class MessageQueue extends EventEmitter {
  constructor(timeoutMs = 600000) {
    super();
    this.messages = [];
    this.waitingResolvers = [];
    this.defaultTimeout = timeoutMs;
    this.closed = false;
  }

  enqueue(message) {
    if (this.closed) return;

    if (this.waitingResolvers.length > 0) {
      const resolver = this.waitingResolvers.shift();
      resolver.resolve(message);
    } else {
      this.messages.push(message);
    }
  }

  async dequeue(timeoutMs = this.defaultTimeout) {
    if (this.closed) {
      throw new Error('Queue is closed');
    }

    return new Promise((resolve, reject) => {
      if (this.messages.length > 0) {
        resolve(this.messages.shift());
        return;
      }

      const resolver = { resolve, reject };
      this.waitingResolvers.push(resolver);

      const timeoutId = setTimeout(() => {
        const index = this.waitingResolvers.indexOf(resolver);
        if (index !== -1) {
          this.waitingResolvers.splice(index, 1);
          reject(new Error('Queue timeout'));
        }
      }, timeoutMs);

      resolver.timeoutId = timeoutId;
    });
  }

  close() {
    this.closed = true;
    this.waitingResolvers.forEach(resolver => {
      clearTimeout(resolver.timeoutId);
      resolver.reject(new Error('Queue closed'));
    });
    this.waitingResolvers = [];
    this.messages = [];
  }
}

// WebSocket连接管理器
class ConnectionRegistry extends EventEmitter {
  constructor(logger) {
    super();
    this.logger = logger;
    this.connections = new Set();
    this.messageQueues = new Map();
  }

  addConnection(websocket, clientInfo) {
    this.connections.add(websocket);
    this.logger.info(`新客户端连接: ${clientInfo.address}`);

    websocket.on('message', data => {
      this._handleIncomingMessage(data.toString());
    });

    websocket.on('close', () => {
      this._removeConnection(websocket);
    });

    websocket.on('error', error => {
      this.logger.error(`WebSocket连接错误: ${error.message}`);
    });

    this.emit('connectionAdded', websocket);
  }

  _removeConnection(websocket) {
    this.connections.delete(websocket);
    this.logger.info('客户端连接断开');

    // 关闭所有相关的消息队列
    this.messageQueues.forEach(queue => queue.close());
    this.messageQueues.clear();

    this.emit('connectionRemoved', websocket);
  }

  _handleIncomingMessage(messageData) {
    try {
      const parsedMessage = JSON.parse(messageData);
      const requestId = parsedMessage.request_id;

      if (!requestId) {
        this.logger.warn('收到无效消息：缺少request_id');
        return;
      }

      const queue = this.messageQueues.get(requestId);
      if (queue) {
        this._routeMessage(parsedMessage, queue);
      } else {
        this.logger.warn(`收到未知请求ID的消息: ${requestId}`);
      }
    } catch (error) {
      this.logger.error('解析WebSocket消息失败');
    }
  }

  _routeMessage(message, queue) {
    const { event_type } = message;

    switch (event_type) {
      case 'response_headers':
      case 'chunk':
      case 'error':
        queue.enqueue(message);
        break;
      case 'stream_close':
        queue.enqueue({ type: 'STREAM_END' });
        break;
      default:
        this.logger.warn(`未知的事件类型: ${event_type}`);
    }
  }

  hasActiveConnections() {
    return this.connections.size > 0;
  }

  getFirstConnection() {
    return this.connections.values().next().value;
  }

  createMessageQueue(requestId) {
    const queue = new MessageQueue();
    this.messageQueues.set(requestId, queue);
    return queue;
  }

  removeMessageQueue(requestId) {
    const queue = this.messageQueues.get(requestId);
    if (queue) {
      queue.close();
      this.messageQueues.delete(requestId);
    }
  }
}

// 请求处理器
class RequestHandler {
  constructor(connectionRegistry, logger) {
    this.connectionRegistry = connectionRegistry;
    this.logger = logger;
  }

  async processRequest(req, res) {
    this.logger.info(`处理请求: ${req.method} ${req.path}`);

    if (!this.connectionRegistry.hasActiveConnections()) {
      return this._sendErrorResponse(res, 503, '没有可用的浏览器连接');
    }

    const requestId = this._generateRequestId();
    const proxyRequest = this._buildProxyRequest(req, requestId);

    const messageQueue = this.connectionRegistry.createMessageQueue(requestId);

    // 标记请求是否已被客户端中断
    let isAborted = false;

    // 监听响应连接关闭事件（客户端在接收响应过程中断开）
    const onClose = () => {
      // 如果已经中断过，则忽略
      if (isAborted) return;

      // 检查响应是否已正常结束
      if (res.writableEnded || res.finished) return;

      isAborted = true;
      this.logger.info(`请求被客户端中断: ${requestId}`);

      // 通过WebSocket通知client端中断请求
      this._sendAbortSignal(requestId);

      // 关闭消息队列
      messageQueue.close();
    };

    // 监听 res 的 close 事件，而不是 req 的
    // res.on('close') 在客户端断开连接时触发（无论响应是否完成）
    res.on('close', onClose);

    try {
      await this._forwardRequest(proxyRequest);
      await this._handleResponse(messageQueue, res, () => isAborted);
    } catch (error) {
      if (!isAborted) {
        this._handleRequestError(error, res);
      }
    } finally {
      res.off('close', onClose);
      this.connectionRegistry.removeMessageQueue(requestId);
    }
  }

  _sendAbortSignal(requestId) {
    const connection = this.connectionRegistry.getFirstConnection();
    if (connection && connection.readyState === 1) {
      // WebSocket.OPEN
      connection.send(
        JSON.stringify({
          request_id: requestId,
          event_type: 'abort',
        }),
      );
      this.logger.info(`已发送中断信号: ${requestId}`);
    }
  }

  _generateRequestId() {
    return `${Date.now()}_${Math.random().toString(36).substring(2, 11)}`;
  }

  _buildProxyRequest(req, requestId) {
    let requestBody = '';
    if (req.body) {
      requestBody = typeof req.body === 'string' ? req.body : JSON.stringify(req.body);
    }

    return {
      path: req.path,
      method: req.method,
      headers: req.headers,
      query_params: req.query,
      body: requestBody,
      request_id: requestId,
    };
  }

  async _forwardRequest(proxyRequest) {
    const connection = this.connectionRegistry.getFirstConnection();
    connection.send(JSON.stringify(proxyRequest));
  }

  async _handleResponse(messageQueue, res, isAbortedFn) {
    // 等待响应头
    const headerMessage = await messageQueue.dequeue();

    if (isAbortedFn && isAbortedFn()) {
      return; // 请求已中断，直接返回
    }

    if (headerMessage.event_type === 'error') {
      return this._sendErrorResponse(res, headerMessage.status || 500, headerMessage.message);
    }

    // 设置响应头
    this._setResponseHeaders(res, headerMessage);

    // 处理流式数据
    await this._streamResponseData(messageQueue, res, isAbortedFn);
  }

  _setResponseHeaders(res, headerMessage) {
    res.status(headerMessage.status || 200);

    const headers = headerMessage.headers || {};
    Object.entries(headers).forEach(([name, value]) => {
      res.set(name, value);
    });
  }

  async _streamResponseData(messageQueue, res, isAbortedFn) {
    while (true) {
      // 检查请求是否已被中断
      if (isAbortedFn && isAbortedFn()) {
        break;
      }

      try {
        const dataMessage = await messageQueue.dequeue();

        if (dataMessage.type === 'STREAM_END') {
          break;
        }

        if (dataMessage.data) {
          res.write(dataMessage.data);
        }
      } catch (error) {
        if (error.message === 'Queue closed') {
          // 队列被关闭（可能是因为请求中断）
          break;
        } else if (error.message === 'Queue timeout') {
          const contentType = res.get('Content-Type') || '';
          if (contentType.includes('text/event-stream')) {
            res.write(': keepalive\n\n');
          } else {
            break;
          }
        } else {
          throw error;
        }
      }
    }

    if (!isAbortedFn || !isAbortedFn()) {
      res.end();
    }
  }

  _handleRequestError(error, res) {
    if (error.message === 'Queue timeout') {
      this._sendErrorResponse(res, 504, '请求超时');
    } else {
      this.logger.error(`请求处理错误: ${error.message}`);
      this._sendErrorResponse(res, 500, `代理错误: ${error.message}`);
    }
  }

  _sendErrorResponse(res, status, message) {
    res.status(status).send(message);
  }
}

// 主服务器类
class ProxyServerSystem extends EventEmitter {
  constructor(config = {}) {
    super();
    this.config = {
      httpPort: 8889,
      wsPort: 9998,
      host: '127.0.0.1',
      ...config,
    };

    this.logger = new LoggingService('ProxyServer');
    this.connectionRegistry = new ConnectionRegistry(this.logger);
    this.requestHandler = new RequestHandler(this.connectionRegistry, this.logger);

    this.httpServer = null;
    this.wsServer = null;
  }

  async start() {
    try {
      await this._startHttpServer();
      await this._startWebSocketServer();

      this.logger.info('代理服务器系统启动完成');
      this.emit('started');
    } catch (error) {
      this.logger.error(`启动失败: ${error.message}`);
      this.emit('error', error);
      throw error;
    }
  }

  async _startHttpServer() {
    const app = this._createExpressApp();
    this.httpServer = http.createServer(app);

    // Handle HTTP server errors
    this.httpServer.on('error', error => {
      this.logger.error(`HTTP Server Error: ${error.message}`);
      this.emit('error', error);
    });

    return new Promise((resolve, reject) => {
      this.httpServer.listen(this.config.httpPort, this.config.host, () => {
        this.logger.info(`HTTP服务器启动: http://${this.config.host}:${this.config.httpPort}`);
        resolve();
      });

      this.httpServer.once('error', error => {
        reject(error);
      });
    });
  }

  _createExpressApp() {
    const app = express();

    // 中间件配置
    app.use(express.json({ limit: '100mb' }));
    app.use(express.urlencoded({ extended: true, limit: '100mb' }));
    app.use(express.raw({ limit: '100mb' }));

    // 所有路由都由请求处理器处理
    app.all(/(.*)/, (req, res) => this.requestHandler.processRequest(req, res));

    return app;
  }

  async _startWebSocketServer() {
    this.wsServer = new WebSocket.Server({
      port: this.config.wsPort,
      host: this.config.host,
    });

    this.wsServer.on('connection', (ws, req) => {
      this.connectionRegistry.addConnection(ws, {
        address: req.socket.remoteAddress,
      });
    });

    this.wsServer.on('error', error => {
      this.logger.error(`WebSocket Server Error: ${error.message}`);
      this.emit('error', error);
    });

    this.logger.info(`WebSocket服务器启动: ws://${this.config.host}:${this.config.wsPort}`);
  }

  async stop() {
    this.logger.info('正在停止代理服务器系统...');

    const closePromises = [];

    if (this.httpServer) {
      closePromises.push(
        new Promise(resolve => {
          this.httpServer.close(() => {
            this.logger.info('HTTP服务器已停止');
            resolve();
          });
        }),
      );
    }

    if (this.wsServer) {
      closePromises.push(
        new Promise(resolve => {
          this.wsServer.close(() => {
            this.logger.info('WebSocket服务器已停止');
            resolve();
          });
        }),
      );
    }

    // 关闭所有活跃连接
    if (this.connectionRegistry) {
      this.connectionRegistry.connections.forEach(ws => {
        ws.terminate();
      });
      this.connectionRegistry.messageQueues.forEach(queue => queue.close());
    }

    await Promise.all(closePromises);
    this.logger.info('代理服务器系统已完全停止');
    this.emit('stopped');
  }
}

// 启动函数
async function initializeServer() {
  const serverSystem = new ProxyServerSystem();

  try {
    await serverSystem.start();
  } catch (error) {
    console.error('服务器启动失败:', error.message);
    process.exit(1);
  }
}

// 模块导出和启动
if (require.main === module) {
  initializeServer();
}

module.exports = { ProxyServerSystem, initializeServer };
