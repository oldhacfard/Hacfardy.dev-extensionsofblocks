
(function (Scratch) {
    "use strict";

    if (!Scratch.extensions.unsandboxed) {
        throw new Error("OLDHACFARD WS requires unsandboxed mode.");
    }

    const VERSION = "2.0.0";

    class OLDHACFARDWebSocket {
        constructor() {
            this.connections = new Map();
            this.events = new Map();
            this.nextId = 1;
            this.destroyed = false;
        }

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

        createConfig(options = {}) {
            return {
                reconnect: options.reconnect ?? true,
                maxRetries: options.maxRetries ?? 10,
                retryDelay: options.retryDelay ?? 1000,
                maxRetryDelay: options.maxRetryDelay ?? 30000,
                heartbeat: options.heartbeat ?? false,
                heartbeatInterval: options.heartbeatInterval ?? 25000,
                timeout: options.timeout ?? 15000,
                maxHistory: options.maxHistory ?? 500,
                protocols: options.protocols
            };
        }

        connect(url, options = {}) {
            if (this.destroyed) {
                throw new Error("Framework destroyed");
            }

            if (typeof url !== "string" ||
                !/^wss?:\/\//i.test(url)) {
                throw new Error("Invalid WebSocket URL");
            }

            const id = "ws_" + this.nextId++;

            const connection = {
                id,
                url,
                config: this.createConfig(options),
                socket: null,
                status: "connecting",
                retries: 0,
                history: [],
                listeners: new Map(),
                stats: {
                    sent: 0,
                    received: 0,
                    bytesSent: 0,
                    bytesReceived: 0,
                    openedAt: null,
                    lastActivity: null
                },
                retryTimer: null,
                heartbeatTimer: null,
                connectionTimer: null,
                manualClose: false,
                generation: 0,
                error: null
            };

            this.connections.set(id, connection);
            this._open(connection);

            return id;
        }

        _emit(connection, event, data) {
            const callbacks = connection.listeners.get(event);

            if (!callbacks) return;

            for (const callback of [...callbacks]) {
                try {
                    callback(data, connection.id);
                } catch (error) {
                    console.error("[OLDHACFARD WS]", error);
                }
            }

            const globalCallbacks = this.events.get(event);

            if (globalCallbacks) {
                for (const callback of [...globalCallbacks]) {
                    try {
                        callback(data, connection.id);
                    } catch (error) {
                        console.error(error);
                    }
                }
            }
        }

        _open(connection) {
            if (this.destroyed || connection.manualClose) return;

            const generation = ++connection.generation;

            connection.status = "connecting";
            connection.error = null;

            this._emit(connection, "connecting", {
                url: connection.url
            });

            try {
                const config = connection.config;

                const socket = config.protocols
                    ? new WebSocket(connection.url, config.protocols)
                    : new WebSocket(connection.url);

                connection.socket = socket;

                connection.connectionTimer = setTimeout(() => {
                    if (generation !== connection.generation) return;

                    if (socket.readyState === WebSocket.CONNECTING) {
                        connection.error = "Connection timeout";
                        try {
                            socket.close();
                        } catch (_) {}
                    }
                }, config.timeout);

                socket.onopen = () => {
                    if (generation !== connection.generation) return;

                    clearTimeout(connection.connectionTimer);

                    connection.status = "open";
                    connection.retries = 0;

                    connection.stats.openedAt = Date.now();
                    connection.stats.lastActivity = Date.now();

                    this._emit(connection, "open", {
                        url: connection.url
                    });

                    this._startHeartbeat(connection);
                };

                socket.onmessage = event => {
                    if (generation !== connection.generation) return;

                    const data = event.data;

                    connection.stats.received++;
                    connection.stats.lastActivity = Date.now();

                    const size = typeof data === "string"
                        ? new TextEncoder().encode(data).length
                        : data?.size || data?.byteLength || 0;

                    connection.stats.bytesReceived += size;

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

                    this._emit(connection, "message", data);
                };

                socket.onerror = () => {
                    if (generation !== connection.generation) return;

                    connection.error = "WebSocket error";

                    this._emit(connection, "error", {
                        message: connection.error
                    });
                };

                socket.onclose = event => {
                    if (generation !== connection.generation) return;

                    clearTimeout(connection.connectionTimer);
                    this._stopHeartbeat(connection);

                    connection.status = "closed";
                    connection.socket = null;

                    this._emit(connection, "close", {
                        code: event.code,
                        reason: event.reason,
                        clean: event.wasClean
                    });

                    if (!connection.manualClose) {
                        this._scheduleReconnect(connection);
                    }
                };

            } catch (error) {
                connection.status = "error";
                connection.error = error.message;

                this._emit(connection, "error", error);
                this._scheduleReconnect(connection);
            }
        }

        _scheduleReconnect(connection) {
            const config = connection.config;

            if (!config.reconnect ||
                connection.manualClose ||
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
                config.retryDelay * Math.pow(2, connection.retries - 1),
                config.maxRetryDelay
            );

            connection.status = "reconnecting";

            this._emit(connection, "reconnecting", {
                retry: connection.retries,
                delay
            });

            clearTimeout(connection.retryTimer);

            connection.retryTimer = setTimeout(() => {
                if (!connection.manualClose) {
                    this._open(connection);
                }
            }, delay);
        }

        _startHeartbeat(connection) {
            this._stopHeartbeat(connection);

            if (!connection.config.heartbeat) return;

            connection.heartbeatTimer = setInterval(() => {
                if (connection.socket?.readyState !== WebSocket.OPEN) {
                    return;
                }

                // Browser WebSocket API does not expose protocol ping.
                // This is an application-level heartbeat message.
                this.send(connection.id, {
                    type: "heartbeat",
                    timestamp: Date.now()
                });

            }, connection.config.heartbeatInterval);
        }

        _stopHeartbeat(connection) {
            clearInterval(connection.heartbeatTimer);
            connection.heartbeatTimer = null;
        }

        send(id, data) {
            const connection = this._get(id);
            const socket = connection.socket;

            if (!socket || socket.readyState !== WebSocket.OPEN) {
                throw new Error("Connection is not open");
            }

            const payload = typeof data === "string"
                ? data
                : JSON.stringify(data);

            socket.send(payload);

            connection.stats.sent++;
            connection.stats.bytesSent +=
                new TextEncoder().encode(payload).length;

            connection.stats.lastActivity = Date.now();

            this._emit(connection, "sent", payload);

            return true;
        }

        sendJSON(id, object) {
            return this.send(id, object);
        }

        disconnect(id, code = 1000, reason = "Manual disconnect") {
            const connection = this._get(id);

            connection.manualClose = true;
            connection.status = "closing";

            clearTimeout(connection.retryTimer);
            clearTimeout(connection.connectionTimer);
            this._stopHeartbeat(connection);

            if (connection.socket) {
                connection.socket.close(code, reason);
            }

            return true;
        }

        remove(id) {
            const connection = this._get(id);

            this.disconnect(id);

            this.connections.delete(id);

            this._emit(connection, "removed", { id });

            return true;
        }

        on(id, event, callback) {
            const connection = this._get(id);

            if (typeof callback !== "function") {
                throw new TypeError("Callback must be a function");
            }

            if (!connection.listeners.has(event)) {
                connection.listeners.set(event, new Set());
            }

            connection.listeners.get(event).add(callback);

            return () => {
                connection.listeners.get(event)?.delete(callback);
            };
        }

        onGlobal(event, callback) {
            if (!this.events.has(event)) {
                this.events.set(event, new Set());
            }

            this.events.get(event).add(callback);

            return () => this.events.get(event)?.delete(callback);
        }

        getStatus(id) {
            return this._get(id).status;
        }

        isConnected(id) {
            return this._get(id).socket?.readyState === WebSocket.OPEN;
        }

        getHistory(id) {
            return [...this._get(id).history];
        }

        getStats(id) {
            const stats = this._get(id).stats;
            return { ...stats };
        }

        getError(id) {
            return this._get(id).error;
        }

        parseJSON(data) {
            return JSON.parse(data);
        }

        stringifyJSON(data) {
            return JSON.stringify(data);
        }

        getConnections() {
            return [...this.connections.keys()];
        }

        _get(id) {
            const connection = this.connections.get(id);

            if (!connection) {
                throw new Error("Unknown connection: " + id);
            }

            return connection;
        }

        destroy() {
            this.destroyed = true;

            for (const connection of this.connections.values()) {
                connection.manualClose = true;

                clearTimeout(connection.retryTimer);
                clearTimeout(connection.connectionTimer);

                this._stopHeartbeat(connection);

                try {
                    connection.socket?.close();
                } catch (_) {}
            }

            this.connections.clear();
            this.events.clear();
        }
    }

    Scratch.extensions.register(
        new OLDHACFARDWebSocket()
    );

})(Scratch);
