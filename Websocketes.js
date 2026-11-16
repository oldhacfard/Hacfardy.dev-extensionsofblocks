
/* ============================================================
   OLDHACFARD WEBSOCKET V3 - ULTIMATE
   Developer: oldhacfard
   Engine: Native WebSocket
   API: OLDHACFARD_WS
   Blocks: NONE
   ============================================================ */

(function (Scratch) {
    "use strict";

    const VERSION = "3.0.0";

    const DEFAULTS = {
        reconnect: true,
        maxRetries: 15,
        retryDelay: 1000,
        maxRetryDelay: 30000,
        retryFactor: 2,
        connectionTimeout: 15000,
        requestTimeout: 10000,
        maxHistory: 1000,
        maxQueue: 1000,
        queueMessages: true,
        heartbeat: false,
        heartbeatInterval: 25000,
        heartbeatMessage: '{"type":"ping"}',
        debug: false
    };

    class OLDHACFARDWebSocket {

        constructor() {
            this.connections = new Map();
            this.globalEvents = new Map();
            this.nextID = 1;
            this.destroyed = false;
            this.createdAt = Date.now();

            this.stats = {
                totalConnections: 0,
                totalMessagesSent: 0,
                totalMessagesReceived: 0,
                totalErrors: 0
            };

            this._log("OLDHACFARD WS V" + VERSION + " initialized");
        }

        /* =========================
           EXTENSION REGISTRATION
        ========================= */

        getInfo() {
            return {
                id: "oldhacfardws",
                name: "OLDHACFARD WebSocket",
                color1: "#7c3aed",
                color2: "#6d28d9",
                color3: "#4c1d95",
                blocks: []
            };
        }

        /* =========================
           LOGGING
        ========================= */

        _log(...args) {
            console.log("[OLDHACFARD WS]", ...args);
        }

        _error(...args) {
            console.error("[OLDHACFARD WS]", ...args);
        }

        /* =========================
           EVENT SYSTEM
        ========================= */

        _emit(connection, event, data) {
            const local = connection
                ? connection.events.get(event)
                : null;

            if (local) {
                for (const callback of [...local]) {
                    try {
                        callback(data, connection.id);
                    } catch (error) {
                        this._error(error);
                    }
                }
            }

            const global = this.globalEvents.get(event);

            if (global) {
                for (const callback of [...global]) {
                    try {
                        callback(data, connection?.id || null);
                    } catch (error) {
                        this._error(error);
                    }
                }
            }

            if (connection) {
                const any = connection.events.get("*");

                if (any) {
                    for (const callback of [...any]) {
                        try {
                            callback({
                                event,
                                data,
                                id: connection.id
                            });
                        } catch (error) {
                            this._error(error);
                        }
                    }
                }
            }
        }

        on(id, event, callback) {
            const connection = this._get(id);

            if (typeof callback !== "function") {
                throw new TypeError("Callback must be a function");
            }

            if (!connection.events.has(event)) {
                connection.events.set(event, new Set());
            }

            connection.events.get(event).add(callback);

            return () => {
                connection.events.get(event)?.delete(callback);
            };
        }

        once(id, event, callback) {
            let unsubscribe;

            unsubscribe = this.on(id, event, (...args) => {
                unsubscribe();
                callback(...args);
            });

            return unsubscribe;
        }

        onAny(id, callback) {
            return this.on(id, "*", callback);
        }

        onGlobal(event, callback) {
            if (!this.globalEvents.has(event)) {
                this.globalEvents.set(event, new Set());
            }

            this.globalEvents.get(event).add(callback);

            return () => {
                this.globalEvents.get(event)?.delete(callback);
            };
        }

        /* =========================
           CONFIGURATION
        ========================= */

        _config(options = {}) {
            return {
                ...DEFAULTS,
                ...options
            };
        }

        /* =========================
           CONNECTION CREATION
        ========================= */

        connect(url, options = {}) {
            if (this.destroyed) {
                throw new Error("Framework has been destroyed");
            }

            if (typeof url !== "string" ||
                !/^wss?:\/\//i.test(url)) {
                throw new Error(
                    "URL must start with ws:// or wss://"
                );
            }

            if (typeof WebSocket === "undefined") {
                throw new Error("WebSocket API unavailable");
            }

            const id = "OHWS_" + this.nextID++;

            const connection = {
                id,
                url,
                config: this._config(options),

                socket: null,
                status: "connecting",

                generation: 0,
                retries: 0,

                manualClose: false,
                permanent: false,

                error: null,
                lastMessage: null,

                history: [],
                queue: [],
                events: new Map(),
                pendingRequests: new Map(),

                timers: {
                    reconnect: null,
                    timeout: null,
                    heartbeat: null
                },

                stats: {
                    sent: 0,
                    received: 0,
                    bytesSent: 0,
                    bytesReceived: 0,
                    openedAt: null,
                    lastActivity: null,
                    reconnects: 0
                }
            };

            this.connections.set(id, connection);
            this.stats.totalConnections++;

            this._open(connection);

            return id;
        }

        /* =========================
           CONNECTION ENGINE
        ========================= */

        _open(connection) {
            if (this.destroyed || connection.manualClose) {
                return;
            }

            const generation = ++connection.generation;

            connection.status = "connecting";
            connection.error = null;

            this._emit(connection, "connecting", {
                url: connection.url
            });

            try {
                const config = connection.config;

                const protocols = config.protocols;

                const socket = protocols
                    ? new WebSocket(connection.url, protocols)
                    : new WebSocket(connection.url);

                connection.socket = socket;

                socket.binaryType = config.binaryType || "arraybuffer";

                connection.timers.timeout = setTimeout(() => {
                    if (generation !== connection.generation) return;

                    if (socket.readyState === WebSocket.CONNECTING) {
                        connection.error = "Connection timeout";

                        try {
                            socket.close();
                        } catch (_) {}
                    }
                }, config.connectionTimeout);

                socket.onopen = () => {
                    if (generation !== connection.generation) return;

                    clearTimeout(connection.timers.timeout);

                    connection.status = "open";
                    connection.retries = 0;

                    connection.stats.openedAt = Date.now();
                    connection.stats.lastActivity = Date.now();

                    this._emit(connection, "open", {
                        id: connection.id,
                        url: connection.url
                    });

                    this._flushQueue(connection);
                    this._startHeartbeat(connection);
                };

                socket.onmessage = event => {
                    if (generation !== connection.generation) return;

                    const data = event.data;

                    const size = typeof data === "string"
                        ? new TextEncoder().encode(data).length
                        : data?.size || data?.byteLength || 0;

                    connection.lastMessage = data;

                    connection.stats.received++;
                    connection.stats.bytesReceived += size;
                    connection.stats.lastActivity = Date.now();

                    this.stats.totalMessagesReceived++;

                    const entry = {
                        data,
                        timestamp: Date.now(),
                        size
                    };

                    connection.history.push(entry);

                    if (connection.history.length >
                        connection.config.maxHistory) {
                        connection.history.shift();
                    }

                    this._resolveResponse(connection, data);

                    this._emit(connection, "message", data);
                };

                socket.onerror = () => {
                    if (generation !== connection.generation) return;

                    connection.error = "WebSocket connection error";

                    this.stats.totalErrors++;

                    this._emit(connection, "error", {
                        message: connection.error
                    });
                };

                socket.onclose = event => {
                    if (generation !== connection.generation) return;

                    clearTimeout(connection.timers.timeout);

                    this._stopHeartbeat(connection);

                    connection.status = "closed";
                    connection.socket = null;

                    this._rejectPending(
                        connection,
                        new Error("Connection closed")
                    );

                    this._emit(connection, "close", {
                        code: event.code,
                        reason: event.reason,
                        clean: event.wasClean
                    });

                    if (!connection.manualClose &&
                        !connection.permanent) {
                        this._scheduleReconnect(connection);
                    }
                };

            } catch (error) {
                connection.status = "error";
                connection.error = error.message;

                this.stats.totalErrors++;

                this._emit(connection, "error", error);

                this._scheduleReconnect(connection);
            }
        }

        /* =========================
           AUTO RECONNECT
        ========================= */

        _scheduleReconnect(connection) {
            const config = connection.config;

            if (!config.reconnect ||
                connection.manualClose ||
                connection.permanent ||
                this.destroyed) {
                return;
            }

            if (connection.retries >= config.maxRetries) {
                connection.status = "failed";

                this._emit(connection, "reconnectFailed", {
                    retries: connection.retries
                });

                return;
            }

            connection.retries++;

            const delay = Math.min(
                config.retryDelay *
                Math.pow(config.retryFactor, connection.retries - 1),
                config.maxRetryDelay
            );

            connection.status = "reconnecting";
            connection.stats.reconnects++;

            this._emit(connection, "reconnecting", {
                attempt: connection.retries,
                delay
            });

            clearTimeout(connection.timers.reconnect);

            connection.timers.reconnect = setTimeout(() => {
                this._open(connection);
            }, delay);
        }

        reconnect(id) {
            const connection = this._get(id);

            connection.manualClose = false;
            connection.permanent = false;
            connection.retries = 0;

            clearTimeout(connection.timers.reconnect);

            if (connection.socket) {
                const old = connection.socket;
                connection.generation++;
                connection.socket = null;

                old.onopen = old.onmessage =
                    old.onerror = old.onclose = null;

                try {
                    old.close();
                } catch (_) {}
            }

            this._open(connection);

            return true;
        }

        /* =========================
           MESSAGE QUEUE
        ========================= */

        _flushQueue(connection) {
            if (connection.socket?.readyState !== WebSocket.OPEN) {
                return;
            }

            while (connection.queue.length) {
                const message = connection.queue.shift();

                try {
                    this.send(connection.id, message);
                } catch (error) {
                    connection.queue.unshift(message);
                    break;
                }
            }

            this._emit(connection, "queueFlushed", {
                remaining: connection.queue.length
            });
        }

        /* =========================
           SEND DATA
        ========================= */

        send(id, data) {
            const connection = this._get(id);

            const socket = connection.socket;

            const payload = typeof data === "string"
                ? data
                : JSON.stringify(data);

            if (!socket || socket.readyState !== WebSocket.OPEN) {
                if (connection.config.queueMessages &&
                    connection.queue.length <
                    connection.config.maxQueue) {

                    connection.queue.push(data);

                    this._emit(connection, "queued", {
                        data
                    });

                    return false;
                }

                throw new Error("WebSocket is not open");
            }

            if (socket.bufferedAmount >
                (connection.config.maxBufferedAmount || 4 * 1024 * 1024)) {
                throw new Error("WebSocket send buffer limit reached");
            }

            socket.send(payload);

            const size = new TextEncoder().encode(payload).length;

            connection.stats.sent++;
            connection.stats.bytesSent += size;
            connection.stats.lastActivity = Date.now();

            this.stats.totalMessagesSent++;

            this._emit(connection, "sent", {
                data,
                bytes: size
            });

            return true;
        }

        sendJSON(id, data) {
            return this.send(id, data);
        }

        sendBinary(id, data) {
            if (!(data instanceof ArrayBuffer) &&
                !ArrayBuffer.isView(data) &&
                !(typeof Blob !== "undefined" && data instanceof Blob)) {
                throw new TypeError("Expected binary data");
            }

            const connection = this._get(id);

            if (!connection.socket ||
                connection.socket.readyState !== WebSocket.OPEN) {
                throw new Error("WebSocket is not open");
            }

            connection.socket.send(data);

            connection.stats.sent++;

            this._emit(connection, "sent", {
                data,
                binary: true
            });

            return true;
        }

        /* =========================
           REQUEST / RESPONSE
        ========================= */

        request(id, data, options = {}) {
            const connection = this._get(id);

            const requestId = "REQ_" +
                Date.now() + "_" +
                Math.random().toString(36).slice(2);

            const timeout = options.timeout ||
                connection.config.requestTimeout;

            const payload = {
                type: "request",
                requestId,
                data
            };

            return new Promise((resolve, reject) => {
                const timer = setTimeout(() => {
                    connection.pendingRequests.delete(requestId);

                    reject(new Error("Request timeout"));
                }, timeout);

                connection.pendingRequests.set(requestId, {
                    resolve,
                    reject,
                    timer
                });

                try {
                    this.send(id, payload);
                } catch (error) {
                    clearTimeout(timer);
                    connection.pendingRequests.delete(requestId);
                    reject(error);
                }
            });
        }

        _resolveResponse(connection, raw) {
            let data;

            try {
                data = typeof raw === "string"
                    ? JSON.parse(raw)
                    : null;
            } catch (_) {
                return;
            }

            if (!data || !data.requestId) return;

            const pending = connection.pendingRequests.get(
                data.requestId
            );

            if (!pending) return;

            clearTimeout(pending.timer);

            connection.pendingRequests.delete(data.requestId);

            if (data.error) {
                pending.reject(new Error(data.error));
            } else {
                pending.resolve(data.data);
            }
        }

        _rejectPending(connection, error) {
            for (const pending of connection.pendingRequests.values()) {
                clearTimeout(pending.timer);
                pending.reject(error);
            }

            connection.pendingRequests.clear();
        }

        /* =========================
           HEARTBEAT
        ========================= */

        _startHeartbeat(connection) {
            this._stopHeartbeat(connection);

            const config = connection.config;

            if (!config.heartbeat) return;

            connection.timers.heartbeat = setInterval(() => {
                if (connection.socket?.readyState !== WebSocket.OPEN) {
                    return;
                }

                try {
                    this.send(
                        connection.id,
                        config.heartbeatMessage
                    );
                } catch (error) {
                    this._error("Heartbeat failed", error);
                }

            }, config.heartbeatInterval);
        }

        _stopHeartbeat(connection) {
            clearInterval(connection.timers.heartbeat);
            connection.timers.heartbeat = null;
        }

        /* =========================
           DISCONNECT / REMOVE
        ========================= */

        disconnect(id, code = 1000, reason = "Manual disconnect") {
            const connection = this._get(id);

            connection.manualClose = true;
            connection.permanent = true;
            connection.status = "closing";

            clearTimeout(connection.timers.reconnect);
            clearTimeout(connection.timers.timeout);

            this._stopHeartbeat(connection);

            connection.queue.length = 0;

            if (connection.socket) {
                try {
                    connection.socket.close(code, reason);
                } catch (error) {
                    connection.error = error.message;
                }
            }

            this._rejectPending(
                connection,
                new Error("Disconnected")
            );

            return true;
        }

        remove(id) {
            const connection = this._get(id);

            this.disconnect(id);

            this.connections.delete(id);

            this._emit(connection, "removed", {
                id
            });

            return true;
        }

        closeAll() {
            for (const id of [...this.connections.keys()]) {
                this.disconnect(id);
            }

            return true;
        }

        /* =========================
           DATA / STATISTICS
        ========================= */

        getStatus(id) {
            return this._get(id).status;
        }

        isConnected(id) {
            return this._get(id).socket?.readyState === WebSocket.OPEN;
        }

        getLastMessage(id) {
            return this._get(id).lastMessage;
        }

        getError(id) {
            return this._get(id).error;
        }

        getHistory(id) {
            return [...this._get(id).history];
        }

        clearHistory(id) {
            this._get(id).history.length = 0;
            return true;
        }

        getQueue(id) {
            return [...this._get(id).queue];
        }

        clearQueue(id) {
            this._get(id).queue.length = 0;
            return true;
        }

        getStats(id) {
            const connection = this._get(id);

            return {
                ...connection.stats,
                status: connection.status,
                retries: connection.retries,
                bufferedAmount:
                    connection.socket?.bufferedAmount || 0,
                queueLength: connection.queue.length,
                historyLength: connection.history.length
            };
        }

        getAllStats() {
            return {
                ...this.stats,
                activeConnections: this.connections.size,
                uptime: Date.now() - this.createdAt,
                version: VERSION
            };
        }

        getConnections() {
            return [...this.connections.keys()];
        }

        getState(id) {
            const c = this._get(id);

            return {
                id: c.id,
                url: c.url,
                status: c.status,
                retries: c.retries,
                connected: this.isConnected(id),
                error: c.error
            };
        }

        /* =========================
           JSON UTILITIES
        ========================= */

        parseJSON(data) {
            return JSON.parse(data);
        }

        stringifyJSON(data) {
            return JSON.stringify(data);
        }

        /* =========================
           INTERNAL ACCESS
        ========================= */

        _get(id) {
            const connection = this.connections.get(id);

            if (!connection) {
                throw new Error("Unknown connection: " + id);
            }

            return connection;
        }

        /* =========================
           CLEANUP
        ========================= */

        destroy() {
            if (this.destroyed) return;

            this.destroyed = true;

            for (const connection of this.connections.values()) {
                connection.manualClose = true;
                connection.permanent = true;

                clearTimeout(connection.timers.reconnect);
                clearTimeout(connection.timers.timeout);

                this._stopHeartbeat(connection);

                this._rejectPending(
                    connection,
                    new Error("Framework destroyed")
                );

                try {
                    connection.socket?.close(1000, "Framework destroyed");
                } catch (_) {}
            }

            this.connections.clear();
            this.globalEvents.clear();

            this._log("Framework destroyed");
        }
    }

    /* =========================
       PUBLIC API
    ========================= */

    const framework = new OLDHACFARDWebSocket();

    const api = Object.freeze({
        version: VERSION,
        connect: framework.connect.bind(framework),
        disconnect: framework.disconnect.bind(framework),
        reconnect: framework.reconnect.bind(framework),
        remove: framework.remove.bind(framework),
        closeAll: framework.closeAll.bind(framework),

        send: framework.send.bind(framework),
        sendJSON: framework.sendJSON.bind(framework),
        sendBinary: framework.sendBinary.bind(framework),
        request: framework.request.bind(framework),

        on: framework.on.bind(framework),
        once: framework.once.bind(framework),
        onAny: framework.onAny.bind(framework),
        onGlobal: framework.onGlobal.bind(framework),

        getStatus: framework.getStatus.bind(framework),
        isConnected: framework.isConnected.bind(framework),
        getLastMessage: framework.getLastMessage.bind(framework),
        getError: framework.getError.bind(framework),

        getHistory: framework.getHistory.bind(framework),
        clearHistory: framework.clearHistory.bind(framework),

        getQueue: framework.getQueue.bind(framework),
        clearQueue: framework.clearQueue.bind(framework),

        getStats: framework.getStats.bind(framework),
        getAllStats: framework.getAllStats.bind(framework),
        getConnections: framework.getConnections.bind(framework),
        getState: framework.getState.bind(framework),

        parseJSON: framework.parseJSON.bind(framework),
        stringifyJSON: framework.stringifyJSON.bind(framework),

        destroy: framework.destroy.bind(framework)
    });

    /* =========================
       GLOBAL EXPORT
    ========================= */

    const root = typeof globalThis !== "undefined"
        ? globalThis
        : window;

    Object.defineProperty(root, "OLDHACFARD_WS", {
        value: api,
        configurable: true,
        enumerable: false,
        writable: false
    });

    /* =========================
       REGISTER EXTENSION
    ========================= */

    Scratch.extensions.register(framework);

    console.log(
        "%c OLDHACFARD WS V" + VERSION + " READY ",
        "background:#7c3aed;color:white;padding:8px;border-radius:5px;font-weight:bold"
    );

    console.log("Global API:", "OLDHACFARD_WS");

    if (!Scratch.extensions.unsandboxed) {
        console.warn(
            "[OLDHACFARD WS] Sandboxed mode: " +
            "the global API is isolated from the editor page."
        );
    }

})(Scratch);
