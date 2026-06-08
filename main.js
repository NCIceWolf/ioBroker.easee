"use strict";
/* eslint camelcase: "off", no-unused-vars: ["error", { "argsIgnorePattern": "^_" }] */

const utils = require("@iobroker/adapter-core");
const https = require("https");
const axios = require("axios").default;
const signalR = require("@microsoft/signalr");
const objEnum = require("./lib/enum.js");

// Configuration constants
const API_URL = "https://api.easee.com";
const SIGNAL_R_URL = "https://streams.easee.com/hubs/chargers";
const MIN_POLL_TIME_ENERGY = 1800; // seconds
const TOKEN_SAFETY_MARGIN = 30000; // milliseconds
const SIGNALR_WATCHDOG_INTERVAL_MS = 60000; // milliseconds
const SIGNALR_SILENCE_THRESHOLD_MS = 360000; // milliseconds
const SIGNALR_STOP_GRACE_MS = 3 * 60 * 1000; // 3 minutes
const SIGNALR_CHARGING_OP_MODES = new Set([2, 3, 6]);
const SIGNALR_NON_CHARGING_OP_MODES = new Set([0, 1, 4, 5, 7, 8]);
const API_TIMEOUT_MS = 30000; // milliseconds
const MAX_RETRY_AFTER_MS = 60000; // milliseconds

class Easee extends utils.Adapter {
  constructor(options) {
    super({
      ...options,
      name: "easee",
    });

    this.on("ready", this.onReady.bind(this));
    this.on("stateChange", this.onStateChange.bind(this));
    this.on("unload", this.onUnload.bind(this));

    // Auth / runtime state
    this.accessToken = "";
    this.refreshToken = "";
    this.expireTime = 0;
    this.polltime = 300;
    this.logtype = false;
    this.roundCounter = 0;
    this.arrCharger = [];
    this.isUnloading = false;
    
    // Concurrency locks
    this.tokenRefreshPromise = undefined;
    this.dynamicCircuitCurrentP = [0, 0, 0]; // P1, P2, P3
    this.isUpdatingCircuit = false;
    this.pendingCircuitUpdate = false;

    // Timer / interval storage
    this.adapterIntervals = {
      readAllStates: undefined,
      updateDynamicCircuitCurrent: undefined,
    };

    // SignalR state
    this.signalRUnloaded = false;
    this.signalRBackoffMs = 1000;
    this.signalRReconnectTimer = undefined;
    this.signalRWatchdog = undefined;
    this.signalRStopGraceTimer = undefined;
    this.signalRStartPromise = undefined;
    this.signalConnection = undefined;
    this.lastSignalRActivity = 0;
    this.chargerOpModes = new Map();

    // Create custom HTTPS agent to be shared with raw axios calls if needed
    this.httpsAgent = new https.Agent({
      rejectUnauthorized: true,
      minVersion: "TLSv1.2",
    });

    // Dedicated axios instance with interceptors
    this.http = this.createHttpClient();
  }

  /**
   * Create a dedicated axios instance with secure defaults and interceptors
   */
  createHttpClient() {
    const client = axios.create({
      baseURL: API_URL,
      httpsAgent: this.httpsAgent,
      timeout: API_TIMEOUT_MS,
    });

    // Request Interceptor: Auto-inject access token
    client.interceptors.request.use(
      async (config) => {
        const requestUrl = config.url || "";

        if (!requestUrl.includes("/api/accounts/login") && !requestUrl.includes("/api/accounts/refresh_token")) {
          await this.ensureValidToken();

          if (this.accessToken) {
            config.headers = config.headers || {};
            config.headers.Authorization = `Bearer ${this.accessToken}`;
          }
        }
        return config;
      },
      (error) => Promise.reject(error)
    );

    // Response Interceptor: Handle 401s and 429s globally
    client.interceptors.response.use(
      (response) => response,
      async (error) => {
        const originalRequest = error.config;
        const status = error.response ? error.response.status : null;

        if (!originalRequest) {
          return Promise.reject(error);
        }

        // Catch 401 Unauthorized and retry exactly once
        if (status === 401 && !originalRequest._retry401) {
          originalRequest._retry401 = true;
          this.log.warn("HTTP 401 caught by interceptor, refreshing token and retrying request...");
          this.expireTime = 0; // Force expiry

          const refreshed = await this.renewToken();
          if (refreshed) {
            originalRequest.headers = originalRequest.headers || {};
            originalRequest.headers.Authorization = `Bearer ${this.accessToken}`;
            return client(originalRequest);
          }
        }

        // Catch 429 Rate Limiting and wait before retrying
        if (status === 429 && !originalRequest._retry429) {
          originalRequest._retry429 = true;
          const retryAfterHeader = error.response.headers ? error.response.headers["retry-after"] : undefined;
          const retryAfterSeconds = Number.parseInt(retryAfterHeader, 10);
          const retryAfterMs = Number.isFinite(retryAfterSeconds)
            ? Math.min(Math.max(retryAfterSeconds * 1000, 1000), MAX_RETRY_AFTER_MS)
            : 5000;
          
          this.log.warn(`HTTP 429 Rate limited. Pausing for ${retryAfterMs / 1000}s before retrying...`);
          await new Promise((resolve) => setTimeout(resolve, retryAfterMs));
          
          return client(originalRequest);
        }

        return Promise.reject(error);
      }
    );

    return client;
  }

  /**
   * Helper to safely set a state using setStateChangedAsync
   * This drastically reduces CPU and DB load by bypassing the event bus if the value hasn't changed.
   * @param {string} id The state ID to update
   * @param {string | number | boolean | null} val The new value to set
   * @param {boolean} ack Whether the state is acknowledged
   */
  async safeSetState(id, val, ack = true) {
    await this.setStateChangedAsync(id, { val, ack });
  }

  /**
   * Helper to sanitize IDs to ensure they contain no forbidden ioBroker characters
   * @param {string} id The string to sanitize
   */
  sanitizeId(id) {
    if (id === undefined || id === null) return "";
    return String(id).replace(/[\]\[*,;'"`<>\?.\s]/g, "_");
  }

  /**
   * Ensure the access token is valid before a write/read request
   * @param {boolean} force Force a token refresh even if not expired
   */
  async ensureValidToken(force = false) {
    if (force || this.expireTime <= Date.now()) {
      if (!this.tokenRefreshPromise) {
        this.log.debug("Access token missing or expired, triggering refresh...");
        this.tokenRefreshPromise = this.renewToken().finally(() => {
          this.tokenRefreshPromise = undefined;
        });
      }
      
      const success = await this.tokenRefreshPromise;
      if (!success) {
        throw new Error("Unable to refresh access token");
      }
    }
  }

  /**
   * Validate charger ID format
   * @param {string | number} chargerId The unique identifier of the charger
   */
  validateChargerId(chargerId) {
    if (chargerId === undefined || chargerId === null || chargerId === "") {
      throw new Error("Invalid charger ID: must not be empty");
    }

    const value = String(chargerId).trim();
    if (!/^[A-Za-z0-9_-]+$/.test(value)) {
      throw new Error(`Invalid charger ID format: ${value}`);
    }

    return value;
  }

  /**
   * Validate site ID format (Easee delivers this ID as integer)
   * @param {string | number} siteId The unique identifier of the site
   */
  validateSiteId(siteId) {
    if (siteId === undefined || siteId === null || siteId === "") {
      throw new Error("Invalid site ID: must not be empty");
    }

    const value = String(siteId).trim();
    if (!/^[A-Za-z0-9_-]+$/.test(value)) {
      throw new Error(`Invalid site ID format: ${value}`);
    }

    return value;
  }

  /**
   * Validate circuit ID format (Easee liefert diese oft als Integer)
   * @param {string | number} circuitId The unique identifier of the circuit
   */
  validateCircuitId(circuitId) {
    if (circuitId === undefined || circuitId === null || circuitId === "") {
      throw new Error("Invalid circuit ID: must not be empty");
    }

    const value = String(circuitId).trim();
    if (!/^[A-Za-z0-9_-]+$/.test(value)) {
      throw new Error(`Invalid circuit ID format: ${value}`);
    }

    return value;
  }

  /**
   * Start SignalR connection with watchdog and reconnect handling
   */
  /**
   * Return whether a charger operation mode requires a live SignalR connection.
   * @param {string | number} opMode Charger operation mode
   */
  isSignalRChargingOpMode(opMode) {
    const numericOpMode = Number(opMode);
    return Number.isFinite(numericOpMode) && SIGNALR_CHARGING_OP_MODES.has(numericOpMode);
  }

  /**
   * Return whether a charger operation mode is explicitly considered non-charging.
   * @param {string | number} opMode Charger operation mode
   */
  isSignalRNonChargingOpMode(opMode) {
    const numericOpMode = Number(opMode);
    return Number.isFinite(numericOpMode) && SIGNALR_NON_CHARGING_OP_MODES.has(numericOpMode);
  }

  /**
   * Check whether at least one known charger is in a mode that should keep SignalR alive.
   */
  hasAnySignalRChargingCharger() {
    for (const opMode of this.chargerOpModes.values()) {
      if (this.isSignalRChargingOpMode(opMode)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Cancel a pending delayed SignalR stop.
   */
  cancelSignalRStopGraceTimer() {
    if (this.signalRStopGraceTimer) {
      clearTimeout(this.signalRStopGraceTimer);
      this.signalRStopGraceTimer = undefined;
      this.log.debug("SignalR stop grace timer cancelled because charging mode is active again");
    }
  }

  /**
   * Schedule SignalR shutdown after the grace timeout if no charger returns to charging mode.
   * @param {string} reason Human-readable reason for logging
   */
  scheduleSignalRStopGraceTimer(reason) {
    if (this.signalRUnloaded || this.isUnloading || !this.config.signalR) return;

    if (this.hasAnySignalRChargingCharger()) {
      this.cancelSignalRStopGraceTimer();
      return;
    }

    if (this.signalRStopGraceTimer) {
      return;
    }

    this.log.debug(
      `${reason}; stopping SignalR in ${Math.round(SIGNALR_STOP_GRACE_MS / 1000)}s if no charger returns to mode 2, 3 or 6`
    );

    this.signalRStopGraceTimer = setTimeout(() => {
      this.signalRStopGraceTimer = undefined;

      if (this.signalRUnloaded || this.isUnloading || this.hasAnySignalRChargingCharger()) {
        return;
      }

      this.stopSignalRConnection("No charger in mode 2, 3 or 6 after grace timeout").catch((err) => {
        this.log.warn(`Failed to stop SignalR after grace timeout: ${this.getErrorMessage(err)}`);
      });
    }, SIGNALR_STOP_GRACE_MS);
  }

  /**
   * Stop SignalR and all related timers/reconnect handling.
   * @param {string} reason Human-readable reason for logging
   */
  async stopSignalRConnection(reason) {
    if (this.signalRReconnectTimer) {
      clearTimeout(this.signalRReconnectTimer);
      this.signalRReconnectTimer = undefined;
    }

    if (this.signalRWatchdog) {
      clearInterval(this.signalRWatchdog);
      this.signalRWatchdog = undefined;
    }

    const connection = this.signalConnection;
    this.signalConnection = undefined;
    this.signalRBackoffMs = 1000;
    this.lastSignalRActivity = 0;

    if (!connection) {
      this.log.debug(`SignalR already stopped: ${reason}`);
      return;
    }

    try {
      this.log.info(`Stopping SignalR connection: ${reason}`);
      await connection.stop();
    } catch (err) {
      this.log.warn(`SignalR stop failed: ${this.getErrorMessage(err)}`);
    }
  }

  /**
   * Update SignalR lifecycle based on a charger's operation mode.
   * SignalR is started/kept alive only while at least one charger is in mode 2, 3 or 6.
   * Once all known chargers leave those modes, shutdown is delayed by SIGNALR_STOP_GRACE_MS.
   * @param {string | number} chargerId The charger identifier
   * @param {string | number} opMode Charger operation mode
   * @param {string} source Source used for logging
   */
  async updateSignalRConnectionForChargerOpMode(chargerId, opMode, source = "unknown") {
    if (!this.config.signalR || this.signalRUnloaded || this.isUnloading) {
      return;
    }

    if (opMode === undefined || opMode === null || opMode === "") {
      return;
    }

    const safeChargerId = this.validateChargerId(chargerId);
    const numericOpMode = Number(opMode);

    if (!Number.isFinite(numericOpMode)) {
      this.log.debug(`Ignoring invalid chargerOpMode from ${source} for ${safeChargerId}: ${opMode}`);
      return;
    }

    this.chargerOpModes.set(safeChargerId, numericOpMode);

    if (this.isSignalRChargingOpMode(numericOpMode)) {
      this.cancelSignalRStopGraceTimer();

      const state = this.signalConnection?.state;
      if (
        state === signalR.HubConnectionState.Connected ||
        state === signalR.HubConnectionState.Connecting ||
        state === signalR.HubConnectionState.Reconnecting ||
        this.signalRStartPromise
      ) {
        this.log.debug(
          `SignalR remains active because charger ${safeChargerId} is in opMode ${numericOpMode} (${source})`
        );
        return;
      }

      this.log.info(
        `Starting SignalR because charger ${safeChargerId} is in opMode ${numericOpMode} (${source})`
      );
      await this.startSignal();
      return;
    }

    const modeText = this.isSignalRNonChargingOpMode(numericOpMode)
      ? "non-charging"
      : "not configured to keep SignalR alive";

    this.log.debug(
      `Charger ${safeChargerId} changed to ${modeText} opMode ${numericOpMode} (${source})`
    );

    if (!this.hasAnySignalRChargingCharger()) {
      this.scheduleSignalRStopGraceTimer(
        "All known chargers are outside SignalR keep-alive modes 2, 3 and 6"
      );
    }
  }

  /**
   * Start SignalR connection with watchdog and reconnect handling.
   * The connection is only started when at least one known charger is in opMode 2, 3 or 6.
   */
  async startSignal() {
    if (this.signalRUnloaded || this.isUnloading || !this.config.signalR) {
      return;
    }

    if (!this.hasAnySignalRChargingCharger()) {
      this.log.debug("SignalR start skipped because no charger is in opMode 2, 3 or 6");
      return;
    }

    const state = this.signalConnection?.state;
    if (
      state === signalR.HubConnectionState.Connected ||
      state === signalR.HubConnectionState.Connecting ||
      state === signalR.HubConnectionState.Reconnecting
    ) {
      return;
    }

    if (this.signalRStartPromise) {
      await this.signalRStartPromise;
      return;
    }

    this.signalRStartPromise = (async () => {
      try {
        await this.ensureValidToken();
      } catch (error) {
        this.log.warn(`SignalR start postponed: ${this.getErrorMessage(error)}`);
        return;
      }

      if (!this.hasAnySignalRChargingCharger() || this.isUnloading) {
        this.log.debug("SignalR start cancelled because charger mode changed during token validation");
        return;
      }

      if (this.signalConnection) {
        try {
          await this.signalConnection.stop();
        } catch (err) {
          this.log.debug(`Ignoring SignalR stop error before restart: ${this.getErrorMessage(err)}`);
        }
        this.signalConnection = undefined;
      }

      const connection = new signalR.HubConnectionBuilder()
        .withUrl(SIGNAL_R_URL, {
          accessTokenFactory: () => this.accessToken,
          transport: signalR.HttpTransportType.WebSockets | signalR.HttpTransportType.ServerSentEvents,
        })
        .withAutomaticReconnect()
        .build();

      this.signalConnection = connection;
      this.lastSignalRActivity = Date.now();

      connection.on("ProductUpdate", (data) => {
        this.handleSignalRProductUpdate(data);
      });

      connection.onreconnecting((err) => {
        this.log.warn(`SignalR reconnecting: ${this.getErrorMessage(err)}`);
      });

      connection.onreconnected(async () => {
        this.log.debug("SignalR reconnected");
        this.lastSignalRActivity = Date.now();

        if (!this.hasAnySignalRChargingCharger()) {
          this.scheduleSignalRStopGraceTimer(
            "SignalR reconnected but no charger is in keep-alive mode 2, 3 or 6"
          );
          return;
        }

        try {
          await this.subscribeAllChargersToSignalR(connection);
        } catch (error) {
          this.log.warn(
            `Failed to re-subscribe chargers after reconnect: ${this.getErrorMessage(error)}`
          );
        }
      });

      connection.onclose(() => {
        if (this.signalRUnloaded || this.isUnloading) {
          return;
        }
        this.handleSignalRDisconnection();
      });

      try {
        await connection.start();
        this.signalRBackoffMs = 1000;
        await this.subscribeAllChargersToSignalR(connection);
      } catch (err) {
        this.log.warn(`SignalR start() failed: ${this.getErrorMessage(err)}`);
        this.handleSignalRDisconnection();
        return;
      }

      this.startSignalRWatchdog();
    })();

    try {
      await this.signalRStartPromise;
    } finally {
      this.signalRStartPromise = undefined;
    }
  }

  /**
   * Handle ProductUpdate event from SignalR
   * @param {Object} data The payload received from SignalR
   */
  handleSignalRProductUpdate(data) {
    this.lastSignalRActivity = Date.now();

    if (!data || !data.id) {
      this.log.warn("Invalid SignalR ProductUpdate: missing data.id");
      return;
    }

    const dataName = objEnum.getNameByEnum(data.id);
    if (!dataName) {
      this.log.debug(`New SignalR-ID, possible new Value: ${data.id}`);
      return;
    }

    // Apply sanitization defensively for base mid
    const safeMid = this.sanitizeId(data.mid);
    const tmpValueId = `${safeMid}${dataName}`;
    const convertedValue = this.convertSignalRValue(data.value, data.dataType);

    this.log.debug(
      `New value over SignalR for: ${tmpValueId}, value: ${convertedValue}`
    );

    this.safeSetState(tmpValueId, convertedValue, true).catch((err) => {
      this.log.error(
        `Failed to set state ${tmpValueId}: ${this.getErrorMessage(err)}`
      );
    });

    if (dataName.endsWith("status.chargerOpMode") || tmpValueId.endsWith("status.chargerOpMode")) {
      this.updateSignalRConnectionForChargerOpMode(safeMid, convertedValue, "SignalR ProductUpdate").catch((err) => {
        this.log.warn(`Failed to update SignalR lifecycle from ProductUpdate: ${this.getErrorMessage(err)}`);
      });
    }
  }

  /**
   * Convert SignalR value based on dataType
   * @param {string | number | boolean} value The raw value from SignalR
   * @param {number} dataType The data type indicator from SignalR
   */
  convertSignalRValue(value, dataType) {
    switch (dataType) {
      case 2: // Boolean
        return value === "1" || value === 1 || value === true;
      case 3: { // Float
        const parsed = parseFloat(value);
        return Number.isNaN(parsed) ? null : parsed;
      }
      case 4: { // Integer
        const parsed = parseInt(value, 10);
        return Number.isNaN(parsed) ? null : parsed;
      }
      default:
        return value;
    }
  }

  /**
   * Subscribe all known chargers to SignalR
   * @param {Object} connection The active SignalR connection object
   */
  async subscribeAllChargersToSignalR(connection) {
    for (const chargerId of this.arrCharger) {
      try {
        await connection.send("SubscribeWithCurrentState", chargerId, true);
        this.log.info(`Charger registered in SignalR: ${chargerId}`);
      } catch (err) {
        this.log.warn(
          `SignalR subscribe for ${chargerId} failed: ${this.getErrorMessage(err)}`
        );
      }
    }
  }

  /**
   * Handle SignalR disconnection with exponential backoff
   */
  handleSignalRDisconnection() {
    if (this.signalRUnloaded || this.isUnloading) return;
    if (!this.config.signalR || !this.hasAnySignalRChargingCharger()) {
      this.log.debug("SignalR reconnect skipped because no charger is in opMode 2, 3 or 6");
      return;
    }
    if (this.signalRReconnectTimer) return;

    const delay = this.signalRBackoffMs || 1000;
    this.log.warn(`SignalR connection closed - restart in ${Math.round(delay / 1000)}s`);
    this.signalRBackoffMs = Math.min(delay * 2, 60000);

    this.signalRReconnectTimer = setTimeout(() => {
      this.signalRReconnectTimer = undefined;

      if (!this.hasAnySignalRChargingCharger() || this.isUnloading) {
        this.log.debug("SignalR restart skipped because charger mode changed during reconnect backoff");
        return;
      }

      this.startSignal().catch((err) => {
        this.log.warn(`SignalR restart failed: ${this.getErrorMessage(err)}`);
      });
    }, delay);
  }

  /**
   * Start SignalR silent-zombie watchdog
   */
  startSignalRWatchdog() {
    if (this.signalRWatchdog) return;

    this.signalRWatchdog = setInterval(() => {
      if (this.signalRUnloaded || this.isUnloading) return;

      if (!this.hasAnySignalRChargingCharger()) {
        this.scheduleSignalRStopGraceTimer(
          "SignalR watchdog found no charger in keep-alive mode 2, 3 or 6"
        );
        return;
      }

      const silenceMs = Date.now() - (this.lastSignalRActivity || 0);

      if (silenceMs > SIGNALR_SILENCE_THRESHOLD_MS) {
        this.log.warn(`SignalR silent for ${Math.round(silenceMs / 1000)}s, forcing reconnect`);
        this.lastSignalRActivity = Date.now();

        if (this.signalConnection) {
          this.signalConnection.stop().catch((err) => {
            this.log.debug(`Ignoring SignalR stop error during watchdog reconnect: ${this.getErrorMessage(err)}`);
          });
        }
      }
    }, SIGNALR_WATCHDOG_INTERVAL_MS);
  }

  /**
   * Initialize the adapter
   */
  async onReady() {
    try {
      await this.safeSetState("info.connection", false, true);

      this.validateConfiguration();
      await this.initializeAdapter();
    } catch (error) {
      this.log.error(`onReady failed: ${this.getErrorMessage(error)}`);
      await this.safeSetState("info.connection", false, true);
    }
  }

  /**
   * Validate adapter configuration
   */
  validateConfiguration() {
    if (!this.config.username || this.config.username === "+49") {
      throw new Error("No username configured");
    }
    if (!this.config.client_secret) {
      throw new Error("No password configured");
    }

    const polltime = Number(this.config.polltime);
    if (!Number.isFinite(polltime) || polltime < 1) {
      this.log.warn("Poll interval too short or invalid, using default 300 seconds");
      this.polltime = 300;
    } else {
      this.polltime = polltime;
    }

    this.logtype = !!this.config.logtype;
  }

  /**
   * Initialize adapter after successful configuration
   */
  async initializeAdapter() {
    this.log.debug("Starting adapter initialization");

    const loginSuccess = await this.login(
      this.config.username,
      this.config.client_secret
    );

    if (!loginSuccess) {
      throw new Error("Login failed");
    }

    await this.setObjectNotExistsAsync("lastUpdate", {
      type: "state",
      common: {
        name: "Last update timestamp",
        type: "string",
        role: "indicator",
        read: true,
        write: false,
      },
      native: {},
    });

    this.arrCharger = [];
    this.roundCounter = 0;

    await this.readAllStates();

    if (this.config.signalR) {
      this.log.debug("SignalR lifecycle is controlled by chargerOpMode; keep-alive modes are 2, 3 and 6");
      if (!this.hasAnySignalRChargingCharger()) {
        this.log.debug("SignalR not started because no charger is currently in opMode 2, 3 or 6");
      }
    }
  }

  /**
   * Clean up and unload the adapter
   * @param {() => void} callback The callback to execute when unload is complete
   */
  onUnload(callback) {
    (async () => {
      try {
        if (this.adapterIntervals.readAllStates) {
          clearTimeout(this.adapterIntervals.readAllStates);
          this.adapterIntervals.readAllStates = undefined;
        }

        if (this.adapterIntervals.updateDynamicCircuitCurrent) {
          clearTimeout(this.adapterIntervals.updateDynamicCircuitCurrent);
          this.adapterIntervals.updateDynamicCircuitCurrent = undefined;
        }

        this.isUnloading = true;
        this.signalRUnloaded = true;

        if (this.signalRReconnectTimer) {
          clearTimeout(this.signalRReconnectTimer);
          this.signalRReconnectTimer = undefined;
        }

        if (this.signalRWatchdog) {
          clearInterval(this.signalRWatchdog);
          this.signalRWatchdog = undefined;
        }

        if (this.signalRStopGraceTimer) {
          clearTimeout(this.signalRStopGraceTimer);
          this.signalRStopGraceTimer = undefined;
        }

        if (this.signalConnection) {
          try {
            await this.signalConnection.stop();
          } catch (err) {
            this.log.debug(`Ignoring SignalR stop error during unload: ${this.getErrorMessage(err)}`);
          }
          this.signalConnection = undefined;
        }

        this.accessToken = "";
        this.refreshToken = "";
        this.expireTime = 0;

        await this.safeSetState("info.connection", false, true);

        this.log.debug("Adapter cleanup completed");
        callback();
      } catch (error) {
        this.log.error(`Error during unload: ${this.getErrorMessage(error)}`);
        callback();
      }
    })();
  }

  /**
   * Main polling loop to read all charger states
   */
  async readAllStates() {
    try {
      await this.ensureValidToken();
      this.log.debug("Reading all states from API");

      const chargers = await this.getAllCharger();
      if (!chargers || !Array.isArray(chargers)) {
        this.log.warn("No chargers found or invalid response from API");
        return;
      }

      this.roundCounter += 1;
      const shouldPollEnergy =
        this.roundCounter >= Math.max(1, Math.ceil(MIN_POLL_TIME_ENERGY / this.polltime));

      if (shouldPollEnergy) {
        this.roundCounter = 0;
      }

      // Process chargers sequentially to respect API rate limits
      for (const charger of chargers) {
        try {
          // eslint-disable-next-line no-await-in-loop
          await this.processCharger(charger, shouldPollEnergy);
        } catch (error) {
          this.log.error(
            `Error processing charger ${charger?.id}: ${this.getErrorMessage(error)}`
          );
        }
      }

      await this.safeSetState("lastUpdate", new Date().toLocaleString("de-DE"), true);
    } catch (error) {
      this.log.error(`readAllStates failed: ${this.getErrorMessage(error)}`);
    } finally {
      if (!this.isUnloading) {
        // Memory Optimization: Using arrow function instead of bind(this)
        this.adapterIntervals.readAllStates = setTimeout(
          () => this.readAllStates(),
          this.polltime * 1000
        );
      }
    }
  }

  /**
   * Process a single charger
   * @param {Object} charger The charger object from the API
   * @param {boolean} shouldPollEnergy Whether to poll energy session data
   */
  async processCharger(charger, shouldPollEnergy = false) {
    if (!charger || !charger.id) {
      throw new Error("Invalid charger object");
    }

    const chargerId = this.validateChargerId(charger.id);

    if (!this.arrCharger.includes(chargerId)) {
      // Execute object creation in parallel for fast initialization
      await Promise.all([
        this.setAllStatusObjects(charger),
        this.setAllConfigObjects(charger)
      ]);
      
      this.arrCharger.push(chargerId);
      this.log.debug(`Initialized new charger: ${chargerId}`);

      if (this.signalConnection && this.signalConnection.state === signalR.HubConnectionState.Connected) {
        try {
          await this.signalConnection.send("SubscribeWithCurrentState", chargerId, true);
          this.log.info(`Charger registered in active SignalR: ${chargerId}`);
        } catch (err) {
          this.log.warn(`SignalR subscribe for ${chargerId} failed: ${this.getErrorMessage(err)}`);
        }
      }
    }

    // Fetch state and config in parallel
    const [chargerState, chargerConfig] = await Promise.all([
      this.getChargerState(chargerId),
      this.getChargerConfig(chargerId),
    ]);

    await this.setNewStatusToCharger(charger, chargerState);
    await this.updateSignalRConnectionForChargerOpMode(chargerId, chargerState?.chargerOpMode, "API poll");
    await this.setConfigStatus(charger, chargerConfig);

    if (shouldPollEnergy) {
      const chargerSession = await this.getChargerSession(chargerId);
      await this.setNewSessionToCharger(charger, chargerSession);
    }
  }

  /**
   * Handle state change events
   * @param {string} id The ID of the state that changed
   * @param {Object} state The new state object
   */
  onStateChange(id, state) {
    if (!state) {
      this.log.debug(`State deleted: ${id}`);
      return;
    }

    this.log.debug(`State changed: ${id} = ${state.val} (ack=${state.ack})`);

    try {
      const parts = id.split(".");
      if (parts.length < 5) {
        this.log.warn(`Invalid state ID format: ${id}`);
        return;
      }

      // Safe to extract because we sanitized it upon object creation
      const chargerId = parts[2];
      const category = parts[3];
      const property = parts[4];

      if (category === "config") {
        this.handleConfigChange(chargerId, property, state).catch((error) => {
          this.log.error(`Error handling config change ${id}: ${this.getErrorMessage(error)}`);
        });
      } else if (category === "control") {
        if (state.ack || state.val !== true) return;
        this.handleControlChange(chargerId, property).catch((error) => {
          this.log.error(`Error handling control command ${id}: ${this.getErrorMessage(error)}`);
        });
      }
    } catch (error) {
      this.log.error(`Error handling state change ${id}: ${this.getErrorMessage(error)}`);
    }
  }

  /**
   * Handle configuration change with strict type casting (Relies on SignalR for ACK)
   * @param {string} chargerId The unique identifier of the charger
   * @param {string} property The configuration property to change
   * @param {Object} state The ioBroker state object containing the new value
   */
  async handleConfigChange(chargerId, property, state) {
    if (state.ack) return;

    try {
      chargerId = this.validateChargerId(chargerId);
      let parsedValue = state.val;

      // Strict type casting to prevent sending malformed payloads
      if (
        property.includes("Current") ||
        property.includes("phaseMode") ||
        property.includes("Brightness")
      ) {
        parsedValue = Number(parsedValue);
        if (Number.isNaN(parsedValue)) {
          throw new Error(`Invalid numeric value provided for ${property}`);
        }
      } else if (
        property === "isEnabled" ||
        property === "smartCharging" ||
        property === "smartButtonEnabled"
      ) {
        parsedValue = Boolean(parsedValue);
      } else if (typeof parsedValue !== "string") {
        parsedValue = String(parsedValue);
      }

      if (property.startsWith("circuitMaxCurrent")) {
        await this.handleCircuitMaxCurrentChange(chargerId, property, parsedValue);
      } else if (property.startsWith("dynamicCircuitCurrent")) {
        await this.handleDynamicCircuitCurrentChange(chargerId, property, parsedValue);
      } else if (property === "isEnabled") {
        await this.changeConfig(chargerId, "enabled", parsedValue);
      } else {
        await this.changeConfig(chargerId, property, parsedValue);
      }
    } catch (error) {
      this.log.error(`Error handling config change ${chargerId}.${property}: ${this.getErrorMessage(error)}`);
    }
  }

  /**
   * Handle circuit max current change
   * @param {string} chargerId The unique identifier of the charger
   * @param {string} property The property name being updated
   * @param {number} value The new maximum current value
   */
  async handleCircuitMaxCurrentChange(chargerId, property, value) {
    try {
      const site = await this.getChargerSite(chargerId);

      if (!site || !Array.isArray(site.circuits) || site.circuits.length === 0) {
        throw new Error("Invalid site data: no circuits found");
      }

      const numericValue = Number(value);
      if (!Number.isFinite(numericValue)) {
        throw new Error(`Invalid circuit max current value: ${value}`);
      }

      this.log.debug(`Updating ${property} to ${numericValue} for circuit ${site.circuits[0].id}`);
      await this.changeMaxCircuitConfig(site.id, site.circuits[0].id, numericValue);
    } catch (error) {
      this.log.error(`Failed to update circuit max current: ${this.getErrorMessage(error)}`);
      throw error;
    }
  }

  /**
   * Handle dynamic circuit current change with blocking debouncer
   * @param {string} chargerId The unique identifier of the charger
   * @param {string} property The property name being updated
   * @param {number} value The new dynamic current value
   */
  async handleDynamicCircuitCurrentChange(chargerId, property, value) {
    try {
      const phaseIndex =
        property === "dynamicCircuitCurrentP1" ? 0
          : property === "dynamicCircuitCurrentP2" ? 1
          : 2;

      const numericValue = Number(value);
      if (!Number.isFinite(numericValue)) {
        throw new Error(`Invalid dynamic circuit current value: ${value}`);
      }

      this.dynamicCircuitCurrentP[phaseIndex] = numericValue;

      if (this.adapterIntervals.updateDynamicCircuitCurrent) {
        clearTimeout(this.adapterIntervals.updateDynamicCircuitCurrent);
      }

      this.adapterIntervals.updateDynamicCircuitCurrent = setTimeout(() => {
        this.executeDynamicCircuitUpdate(chargerId).catch((err) => {
          this.log.error(`Failed to execute dynamic circuit update: ${this.getErrorMessage(err)}`);
        });
      }, 500);

    } catch (error) {
      this.log.error(`Error handling dynamic circuit current change: ${this.getErrorMessage(error)}`);
    }
  }

  /**
   * Protected executor for Dynamic Circuit Update
   * @param {string} chargerId The unique identifier of the charger
   */
  async executeDynamicCircuitUpdate(chargerId) {
    if (this.isUpdatingCircuit) {
      this.pendingCircuitUpdate = true;
      return;
    }

    this.isUpdatingCircuit = true;
    try {
      const site = await this.getChargerSite(chargerId);

      if (!site || !Array.isArray(site.circuits) || site.circuits.length === 0) {
        throw new Error("Invalid site data: no circuits found");
      }

      await this.changeCircuitConfig(site.id, site.circuits[0].id);
      this.pendingCircuitUpdate = false;
    } finally {
      this.isUpdatingCircuit = false;
      if (this.pendingCircuitUpdate) {
        this.executeDynamicCircuitUpdate(chargerId);
      }
    }
  }

  /**
   * Handle control commands
   * @param {string} chargerId The unique identifier of the charger
   * @param {string} command The control command to execute
   */
  async handleControlChange(chargerId, command) {
    try {
      chargerId = this.validateChargerId(chargerId);
      const controlMap = {
        start: () => this.startCharging(chargerId),
        stop: () => this.stopCharging(chargerId),
        pause: () => this.pauseCharging(chargerId),
        resume: () => this.resumeCharging(chargerId),
        reboot: () => this.rebootCharging(chargerId),
      };

      const handler = controlMap[command];
      if (!handler) {
        this.log.warn(`Unknown control command: ${command}`);
        return;
      }

      this.log.debug(`Executing control: ${command} on charger ${chargerId}`);
      await handler();

      // Reset button state on success (stateless trigger)
      await this.safeSetState(`${chargerId}.control.${command}`, false, true);
    } catch (error) {
      this.log.error(`Error handling control command ${command}: ${this.getErrorMessage(error)}`);
    }
  }

  /**
   * Set charger status values
   * @param {Object} charger The charger object
   * @param {Object} chargerStates The current states of the charger
   */
  async setNewStatusToCharger(charger, chargerStates) {
    try {
      if (!chargerStates || typeof chargerStates !== "object") {
        this.log.warn(`Invalid charger state data for charger ${charger?.id}`);
        return;
      }

      const baseId = this.sanitizeId(charger.id);
      const stateUpdates = [
        [`${baseId}.name`, charger.name],
        [`${baseId}.status.cableLocked`, chargerStates.cableLocked],
        [`${baseId}.status.chargerOpMode`, chargerStates.chargerOpMode],
        [`${baseId}.status.totalPower`, chargerStates.totalPower],
        [`${baseId}.status.wiFiRSSI`, chargerStates.wiFiRSSI],
        [`${baseId}.status.chargerFirmware`, chargerStates.chargerFirmware],
        [`${baseId}.status.reasonForNoCurrent`, chargerStates.reasonForNoCurrent],
        [`${baseId}.status.voltage`, chargerStates.voltage],
        [`${baseId}.status.outputCurrent`, chargerStates.outputCurrent],
        [`${baseId}.status.isOnline`, chargerStates.isOnline],
        [`${baseId}.status.wiFiAPEnabled`, chargerStates.wiFiAPEnabled],
        [`${baseId}.status.ledMode`, chargerStates.ledMode],
        [`${baseId}.status.lifetimeEnergy`, chargerStates.lifetimeEnergy],
        [`${baseId}.status.energyPerHour`, chargerStates.energyPerHour],
        [`${baseId}.status.inCurrentT2`, chargerStates.inCurrentT2],
        [`${baseId}.status.inCurrentT3`, chargerStates.inCurrentT3],
        [`${baseId}.status.inCurrentT4`, chargerStates.inCurrentT4],
        [`${baseId}.status.inCurrentT5`, chargerStates.inCurrentT5],
        [`${baseId}.status.inVoltageT1T2`, chargerStates.inVoltageT1T2],
        [`${baseId}.status.inVoltageT1T3`, chargerStates.inVoltageT1T3],
        [`${baseId}.status.inVoltageT1T4`, chargerStates.inVoltageT1T4],
        [`${baseId}.status.inVoltageT1T5`, chargerStates.inVoltageT1T5],
        [`${baseId}.status.inVoltageT2T3`, chargerStates.inVoltageT2T3],
        [`${baseId}.status.inVoltageT2T4`, chargerStates.inVoltageT2T4],
        [`${baseId}.status.inVoltageT2T5`, chargerStates.inVoltageT2T5],
        [`${baseId}.status.inVoltageT3T4`, chargerStates.inVoltageT3T4],
        [`${baseId}.status.inVoltageT3T5`, chargerStates.inVoltageT3T5],
        [`${baseId}.status.inVoltageT4T5`, chargerStates.inVoltageT4T5],
        [`${baseId}.config.dynamicChargerCurrent`, chargerStates.dynamicChargerCurrent],
        [`${baseId}.config.dynamicCircuitCurrentP1`, chargerStates.dynamicCircuitCurrentP1],
        [`${baseId}.config.dynamicCircuitCurrentP2`, chargerStates.dynamicCircuitCurrentP2],
        [`${baseId}.config.dynamicCircuitCurrentP3`, chargerStates.dynamicCircuitCurrentP3],
        [`${baseId}.config.smartCharging`, chargerStates.smartCharging],
      ];

      await Promise.all(
        stateUpdates.map(([id, value]) =>
          this.safeSetState(id, value, true).catch((err) => {
            this.log.warn(`Failed to set state ${id}: ${this.getErrorMessage(err)}`);
          })
        )
      );
    } catch (error) {
      this.log.error(`Error setting charger status: ${this.getErrorMessage(error)}`);
    }
  }

  /**
   * Set charger configuration values
   * @param {Object} charger The charger object
   * @param {Object} chargerConfig The current configuration of the charger
   */
  async setConfigStatus(charger, chargerConfig) {
    try {
      if (!chargerConfig || typeof chargerConfig !== "object") {
        this.log.warn(`Invalid charger config data for charger ${charger?.id}`);
        return;
      }

      const baseId = this.sanitizeId(charger.id);
      const stateUpdates = [
        [`${baseId}.config.isEnabled`, chargerConfig.isEnabled],
        [`${baseId}.config.phaseMode`, chargerConfig.phaseMode],
        [`${baseId}.config.ledStripBrightness`, chargerConfig.ledStripBrightness],
        [`${baseId}.config.smartButtonEnabled`, chargerConfig.smartButtonEnabled],
        [`${baseId}.config.wiFiSSID`, chargerConfig.wiFiSSID],
        [`${baseId}.config.maxChargerCurrent`, chargerConfig.maxChargerCurrent],
        [`${baseId}.config.circuitMaxCurrentP1`, chargerConfig.circuitMaxCurrentP1],
        [`${baseId}.config.circuitMaxCurrentP2`, chargerConfig.circuitMaxCurrentP2],
        [`${baseId}.config.circuitMaxCurrentP3`, chargerConfig.circuitMaxCurrentP3],
      ];
      
      await Promise.all(
        stateUpdates.map(([id, value]) =>
          this.safeSetState(id, value, true).catch((err) => {
            this.log.warn(`Failed to set config state ${id}: ${this.getErrorMessage(err)}`);
          })
        )
      );
    } catch (error) {
      this.log.error(`Error setting charger config: ${this.getErrorMessage(error)}`);
    }
  }

  /**
   * API: Login and get access token
   * @param {string} username The Easee account username
   * @param {string} password The Easee account password
   */
  async login(username, password) {
    try {
      if (!username || !password) {
        throw new Error("Username and password required");
      }

      this.log.debug("Attempting login");
      const response = await axios.post(
        `${API_URL}/api/accounts/login`,
        { userName: username, password },
        { httpsAgent: this.httpsAgent, timeout: API_TIMEOUT_MS }
      );

      if (!response?.data?.accessToken || !response?.data?.refreshToken) {
        throw new Error("Login response does not contain tokens");
      }

      this.accessToken = response.data.accessToken;
      this.refreshToken = response.data.refreshToken;
      this.expireTime = Date.now() + (Number(response.data.expiresIn || 0) * 1000 - TOKEN_SAFETY_MARGIN);

      this.log.debug("Login successful");
      this.log.debug(`Token expires in ${response.data.expiresIn}s (${Math.round((this.expireTime - Date.now()) / 1000)}s remaining)`);
      
      await this.safeSetState("info.connection", true, true);
      return true;
    } catch (error) {
      this.log.error(`Login failed: ${this.getErrorMessage(error)}`);
      await this.safeSetState("info.connection", false, true);
      return false;
    }
  }

  /**
   * API: Refresh access token
   */
  async renewToken() {
    try {
      if (!this.accessToken || !this.refreshToken) {
        this.log.debug("No tokens available, performing full login");
        return await this.login(this.config.username, this.config.client_secret);
      }

      this.log.debug("Refreshing token");
      const response = await axios.post(
        `${API_URL}/api/accounts/refresh_token`,
        { accessToken: this.accessToken, refreshToken: this.refreshToken },
        { httpsAgent: this.httpsAgent, timeout: API_TIMEOUT_MS }
      );

      if (!response?.data?.accessToken || !response?.data?.refreshToken) {
        throw new Error("Refresh response does not contain tokens");
      }

      this.accessToken = response.data.accessToken;
      this.refreshToken = response.data.refreshToken;
      this.expireTime = Date.now() + (Number(response.data.expiresIn || 0) * 1000 - TOKEN_SAFETY_MARGIN);
        
      if (this.logtype) this.log.debug("Token refreshed successfully");

      await this.safeSetState("info.connection", true, true);
      return true;
    } catch (error) {
      const status = error?.response?.status;
      this.log.warn(`Token refresh failed (HTTP ${status || "?"}): ${this.getErrorMessage(error)}`);
      
      if (status >= 400 && status < 500) {
        this.log.debug("Refresh token invalid, attempting full login");
        const loginSuccess = await this.login(this.config.username, this.config.client_secret);
        if (loginSuccess) return true;
        this.log.error("Full login also failed after refresh token error");
      }

      this.expireTime = 0;
      await this.safeSetState("info.connection", false, true);
      return false;
    }
  }

  /**
   * Helper: GET logic
   * @param {string} path The API endpoint path
   * @param {string} context A descriptive context for logging
   */
  async _apiGet(path, context) {
    try {
      const response = await this.http.get(path);
      this.log.debug(`${context}: success`);
      return response.data;
    } catch (error) {
      const status = error?.response?.status;
      throw new Error(`${context}: ${status ? `HTTP ${status}` : "request failed"} - ${this.getErrorMessage(error)}`);
    }
  }

  /**
   * Helper: POST logic
   * @param {string} path The API endpoint path
   * @param {Object} payload The data payload to post
   * @param {string} context A descriptive context for logging
   */
  async _apiPost(path, payload, context) {
    try {
      const response = await this.http.post(path, payload);
      this.log.debug(`${context}: success`);
      return response.data;
    } catch (error) {
      const status = error?.response?.status;
      throw new Error(`${context}: ${status ? `HTTP ${status}` : "request failed"} - ${this.getErrorMessage(error)}`);
    }
  }

  /**
   * Get all chargers
   */
  async getAllCharger() {
    try {
      return await this._apiGet("/api/chargers", "getAllCharger");
    } catch (error) {
      this.log.error(`Failed to get chargers: ${this.getErrorMessage(error)}`);
      return undefined;
    }
  }

  /**
   * Get charger state
   * @param {string} chargerId The unique identifier of the charger
   */
  async getChargerState(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    const encodedChargerId = encodeURIComponent(chargerId);
    return await this._apiGet(`/api/chargers/${encodedChargerId}/state`, `getChargerState(${chargerId})`);
  }

  /**
   * Get charger configuration
   * @param {string} chargerId The unique identifier of the charger
   */
  async getChargerConfig(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    const encodedChargerId = encodeURIComponent(chargerId);
    return await this._apiGet(`/api/chargers/${encodedChargerId}/config`, `getChargerConfig(${chargerId})`);
  }

  /**
   * Get charger site information
   * @param {string} chargerId The unique identifier of the charger
   */
  async getChargerSite(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    const encodedChargerId = encodeURIComponent(chargerId);
    return await this._apiGet(`/api/chargers/${encodedChargerId}/site`, `getChargerSite(${chargerId})`);
  }

  /**
   * Get charger session data
   * @param {string} chargerId The unique identifier of the charger
   */
  async getChargerSession(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    const encodedChargerId = encodeURIComponent(chargerId);
    return await this._apiGet(`/api/sessions/charger/${encodedChargerId}/monthly`, `getChargerSession(${chargerId})`);
  }

  /**
   * Start charging
   * @param {string} chargerId The unique identifier of the charger
   */
  async startCharging(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    const encodedChargerId = encodeURIComponent(chargerId);
    try {
      await this._apiPost(`/api/chargers/${encodedChargerId}/commands/start_charging`, {}, `startCharging(${chargerId})`);
      this.log.debug(`Start charging successful for ${chargerId}`);
    } catch (error) {
      this.log.error(`Start charging failed for ${chargerId}: ${this.getErrorMessage(error)}`);
      throw error;
    }
  }

  /**
   * Stop charging
   * @param {string} chargerId The unique identifier of the charger
   */
  async stopCharging(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    const encodedChargerId = encodeURIComponent(chargerId);
    try {
      await this._apiPost(`/api/chargers/${encodedChargerId}/commands/stop_charging`, {}, `stopCharging(${chargerId})`);
      this.log.debug(`Stop charging successful for ${chargerId}`);
    } catch (error) {
      this.log.error(`Stop charging failed for ${chargerId}: ${this.getErrorMessage(error)}`);
      throw error;
    }
  }

  /**
   * Pause charging
   * @param {string} chargerId The unique identifier of the charger
   */
  async pauseCharging(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    const encodedChargerId = encodeURIComponent(chargerId);
    try {
      await this._apiPost(`/api/chargers/${encodedChargerId}/commands/pause_charging`, {}, `pauseCharging(${chargerId})`);
      this.log.debug(`Pause charging successful for ${chargerId}`);
    } catch (error) {
      this.log.error(`Pause charging failed for ${chargerId}: ${this.getErrorMessage(error)}`);
      throw error;
    }
  }

  /**
   * Resume charging
   * @param {string} chargerId The unique identifier of the charger
   */
  async resumeCharging(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    const encodedChargerId = encodeURIComponent(chargerId);
    try {
      await this._apiPost(`/api/chargers/${encodedChargerId}/commands/resume_charging`, {}, `resumeCharging(${chargerId})`);
      this.log.debug(`Resume charging successful for ${chargerId}`);
    } catch (error) {
      this.log.error(`Resume charging failed for ${chargerId}: ${this.getErrorMessage(error)}`);
      throw error;
    }
  }

  /**
   * Reboot charger
   * @param {string} chargerId The unique identifier of the charger
   */
  async rebootCharging(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    const encodedChargerId = encodeURIComponent(chargerId);
    try {
      await this._apiPost(`/api/chargers/${encodedChargerId}/commands/reboot`, {}, `rebootCharging(${chargerId})`);
      this.log.debug(`Reboot successful for ${chargerId}`);
    } catch (error) {
      this.log.error(`Reboot failed for ${chargerId}: ${this.getErrorMessage(error)}`);
      throw error;
    }
  }

  /**
   * Change charger configuration
   * @param {string} chargerId The unique identifier of the charger
   * @param {string} configKey The configuration key to update
   * @param {string | number | boolean} value The new value to set
   */
  async changeConfig(chargerId, configKey, value) {
    chargerId = this.validateChargerId(chargerId);
    const encodedChargerId = encodeURIComponent(chargerId);
    try {
      this.log.debug(`Updating charger config: ${configKey} = ${value}`);
      await this._apiPost(`/api/chargers/${encodedChargerId}/settings`, { [configKey]: value }, `changeConfig(${chargerId}, ${configKey})`);
      this.log.debug(`Config update successful: ${configKey} = ${value}`);
    } catch (error) {
      this.log.error(`Config update failed: ${this.getErrorMessage(error)}`);
      throw error;
    }
  }

  /**
   * Change circuit maximum current
   * @param {string} siteId The unique identifier of the site
   * @param {string} circuitId The unique identifier of the circuit
   * @param {number} value The new maximum circuit current
   */
  async changeMaxCircuitConfig(siteId, circuitId, _value) {
    siteId = this.validateSiteId(siteId);
    circuitId = this.validateCircuitId(circuitId);

    try {
      this.log.debug(`Updating circuit max current to ${value}`);
      await this._apiPost(
        `/api/sites/${siteId}/circuits/${circuitId}/settings`,
        { maxCircuitCurrentP1: value, maxCircuitCurrentP2: value, maxCircuitCurrentP3: value },
        `changeMaxCircuitConfig(${siteId}, ${circuitId})`
      );
      this.log.debug(`Circuit max current update successful: ${value}A`);
    } catch (error) {
      this.log.error(`Circuit max current update failed: ${this.getErrorMessage(error)}`);
      throw error;
    }
  }

  /**
   * Change dynamic circuit current
   * @param {string} siteId The unique identifier of the site
   * @param {string} circuitId The unique identifier of the circuit
   */
  async changeCircuitConfig(siteId, circuitId) {
    siteId = this.validateSiteId(siteId);
    circuitId = this.validateCircuitId(circuitId);

    try {
      const payload = {
        dynamicCircuitCurrentP1: this.dynamicCircuitCurrentP[0],
        dynamicCircuitCurrentP2: this.dynamicCircuitCurrentP[1],
        dynamicCircuitCurrentP3: this.dynamicCircuitCurrentP[2],
      };
      this.log.debug(`Updating dynamic circuit current: P1=${payload.dynamicCircuitCurrentP1}, P2=${payload.dynamicCircuitCurrentP2}, P3=${payload.dynamicCircuitCurrentP3}`);
      await this._apiPost(`/api/sites/${siteId}/circuits/${circuitId}/settings`, payload, `changeCircuitConfig(${siteId}, ${circuitId})`);
      this.log.debug("Dynamic circuit current update successful");

      this.dynamicCircuitCurrentP = [0, 0, 0];
    } catch (error) {
      this.log.error(`Dynamic circuit current update failed: ${this.getErrorMessage(error)}`);
      throw error;
    }
  }

  /**
   * Create status objects for a charger
   * @param {Object} charger The charger object
   */
  async setAllStatusObjects(charger) {
    try {
      const baseId = this.sanitizeId(charger.id);
      const promises = [];

      const controlButtons = [
        { name: "start", displayName: "Start charging" },
        { name: "stop", displayName: "Stop charging" },
        { name: "pause", displayName: "Pause charging" },
        { name: "resume", displayName: "Resume charging" },
        { name: "reboot", displayName: "Reboot Charger" },
      ];
      
      for (const button of controlButtons) {
        promises.push(
          this.setObjectNotExistsAsync(`${baseId}.control.${button.name}`, {
            type: "state",
            common: { name: button.displayName, type: "boolean", role: "button", read: false, write: true, def: false },
            native: {},
          }).then(() => {
            this.subscribeStates(`${baseId}.control.${button.name}`);
            return this.safeSetState(`${baseId}.control.${button.name}`, false, true);
          })
        );
      }

      promises.push(
        this.setObjectNotExistsAsync(`${baseId}.id`, {
          type: "state",
          common: { name: "Charger ID", type: "string", role: "info.name", read: true, write: false },
          native: {},
        }).then(() => this.safeSetState(`${baseId}.id`, charger.id, true))
      );
      
      promises.push(
        this.setObjectNotExistsAsync(`${baseId}.name`, {
          type: "state",
          common: { name: "Charger Name", type: "string", role: "info.name", read: true, write: false },
          native: {},
        })
      );
      
      const statusObjects = [
        { name: "cableLocked", displayName: "Cable lock state", type: "boolean", role: "sensor.lock" },
        { name: "chargerOpMode", displayName: "Charger operation mode", type: "number", role: "value", states: { 0: "Offline", 1: "Disconnected", 2: "AwaitingStart", 3: "Charging", 4: "Completed", 5: "Error", 6: "ReadyToCharge", 7: "AwaitingAuthentication", 8: "DeAuthenticating" } },
        { name: "totalPower", displayName: "Total power", type: "number", role: "value.power", unit: "kW" },
        { name: "wiFiRSSI", displayName: "WiFi signal strength", type: "number", role: "value", unit: "dBm" },
        { name: "chargerFirmware", displayName: "Modem firmware version", type: "string", role: "info.firmware" },
        { name: "reasonForNoCurrent", displayName: "Reason for no current", type: "number", role: "value" },
        { name: "voltage", displayName: "Voltage", type: "number", role: "value.voltage", unit: "V" },
        { name: "outputCurrent", displayName: "Output current", type: "number", role: "value.current", unit: "A" },
        { name: "isOnline", displayName: "Charger online", type: "boolean", role: "indicator.reachable" },
        { name: "wiFiAPEnabled", displayName: "WiFi AP enabled", type: "boolean", role: "indicator" },
        { name: "ledMode", displayName: "LED mode", type: "number", role: "value" },
        { name: "lifetimeEnergy", displayName: "Lifetime energy", type: "number", role: "value.power.consumption", unit: "kWh" },
        { name: "energyPerHour", displayName: "Energy per hour", type: "number", role: "value.power.consumption", unit: "kWh" },
        { name: "fatalErrorCode", displayName: "Fatal error code", type: "number", role: "value" },
        { name: "chargingSessionStart", displayName: "Charging session start", type: "string", role: "value.datetime" },
        { name: "connectedToCloud", displayName: "Connected to cloud", type: "boolean", role: "indicator.connected" },
        { name: "cloudDisconnectReason", displayName: "Cloud disconnect reason", type: "string", role: "value" },
      ];
      
      for (const obj of statusObjects) {
        const common = { name: obj.displayName, type: obj.type, role: obj.role, read: true, write: false };
        if (obj.unit) common.unit = obj.unit;
        if (obj.states) common.states = obj.states;

        promises.push(this.setObjectNotExistsAsync(`${baseId}.status.${obj.name}`, { type: "state", common, native: {} }));
      }

      for (let i = 2; i <= 5; i++) {
        promises.push(this.setObjectNotExistsAsync(`${baseId}.status.inCurrentT${i}`, {
          type: "state",
          common: { name: `Current RMS input T${i}`, type: "number", role: "value.current", read: true, write: false, unit: "A" },
          native: {},
        }));
      }

      const voltagePairs = [[1, 2], [1, 3], [1, 4], [1, 5], [2, 3], [2, 4], [2, 5], [3, 4], [3, 5], [4, 5]];

      for (const [t1, t2] of voltagePairs) {
        promises.push(this.setObjectNotExistsAsync(`${baseId}.status.inVoltageT${t1}T${t2}`, {
          type: "state",
          common: { name: `Voltage between T${t1} and T${t2}`, type: "number", role: "value.voltage", read: true, write: false, unit: "V" },
          native: {},
        }));
      }

      promises.push(this.setObjectNotExistsAsync(`${baseId}.status.TempMax`, {
        type: "state",
        common: { name: "Maximum temperature (SignalR only)", type: "number", role: "value.temperature.max", read: true, write: false },
        native: {},
      }));

      promises.push(this.setObjectNotExistsAsync(`${baseId}.config.wiFiMACAddress`, {
        type: "state",
        common: { name: "WiFi MAC address", type: "string", role: "text", read: true, write: false },
        native: {},
      }));

      await Promise.all(promises);
    } catch (error) {
      this.log.error(`Error creating status objects: ${this.getErrorMessage(error)}`);
    }
  }

  /**
   * Create configuration objects for a charger
   * @param {Object} charger The charger object
   */
  async setAllConfigObjects(charger) {
    try {
      const baseId = this.sanitizeId(charger.id);
      const configObjects = [
        { name: "isEnabled", displayName: "Charger enabled", type: "boolean", role: "switch.enabled" },
        { name: "phaseMode", displayName: "Phase mode", type: "number", role: "level" },
        { name: "maxChargerCurrent", displayName: "Max charger current", type: "number", role: "level.current", unit: "A" },
        { name: "dynamicChargerCurrent", displayName: "Dynamic charger current", type: "number", role: "level.current", unit: "A" },
        { name: "dynamicCircuitCurrentP1", displayName: "Dynamic circuit current P1", type: "number", role: "level.current", unit: "A" },
        { name: "dynamicCircuitCurrentP2", displayName: "Dynamic circuit current P2", type: "number", role: "level.current", unit: "A" },
        { name: "dynamicCircuitCurrentP3", displayName: "Dynamic circuit current P3", type: "number", role: "level.current", unit: "A" },
        { name: "circuitMaxCurrentP1", displayName: "Circuit max current P1", type: "number", role: "level.current", unit: "A" },
        { name: "circuitMaxCurrentP2", displayName: "Circuit max current P2", type: "number", role: "level.current", unit: "A" },
        { name: "circuitMaxCurrentP3", displayName: "Circuit max current P3", type: "number", role: "level.current", unit: "A" },
        { name: "ledStripBrightness", displayName: "LED strip brightness", type: "number", role: "level.brightness" },
        { name: "smartCharging", displayName: "Smart charging enabled", type: "boolean", role: "switch.enable" },
        { name: "smartButtonEnabled", displayName: "Smart button enabled", type: "boolean", role: "switch.enable" },
        { name: "wiFiSSID", displayName: "WiFi SSID", type: "string", role: "text" },
      ];
      
      const promises = configObjects.map(async (obj) => {
        const common = { name: obj.displayName, type: obj.type, role: obj.role, read: true, write: true };
        if (obj.unit) common.unit = obj.unit;

        const stateId = `${baseId}.config.${obj.name}`;
        await this.setObjectNotExistsAsync(stateId, { type: "state", common, native: {} });
        this.subscribeStates(stateId);
      });

      await Promise.all(promises);
    } catch (error) {
      this.log.error(`Error creating config objects: ${this.getErrorMessage(error)}`);
    }
  }

  /**
   * Set charger session data
   * @param {Object} charger The charger object
   * @param {Array} chargerSessions The array of session data objects
   */
  async setNewSessionToCharger(charger, chargerSessions) {
    try {
      if (!Array.isArray(chargerSessions)) {
        this.log.warn("Invalid charger sessions data");
        return;
      }

      const baseId = this.sanitizeId(charger.id);
      const promises = [];

      for (const session of chargerSessions) {
        if (!session?.year || !session?.month) continue;

        const sessionPath = `${baseId}.session.${session.year}.${session.month}`;
        
        promises.push(
          this.setObjectNotExistsAsync(`${sessionPath}.totalEnergyUsage`, {
            type: "state",
            common: { name: "Total energy usage", type: "number", role: "value.power.consumption", read: true, write: false, unit: "kWh" },
            native: {},
          }).then(() => this.safeSetState(`${sessionPath}.totalEnergyUsage`, session.totalEnergyUsage, true))
        );
        
        promises.push(
          this.setObjectNotExistsAsync(`${sessionPath}.totalCost`, {
            type: "state",
            common: { name: "Total cost", type: "number", role: "value.money", read: true, write: false },
            native: {},
          }).then(() => this.safeSetState(`${sessionPath}.totalCost`, session.totalCost, true))
        );
      }

      const yearTotals = {};
      for (const session of chargerSessions) {
        if (!session?.year) continue;
        yearTotals[session.year] = (yearTotals[session.year] || 0) + Number(session.totalEnergyUsage || 0);
      }

      for (const [year, total] of Object.entries(yearTotals)) {
        const yearPath = `${baseId}.session.${year}.total_year`;
        promises.push(
          this.setObjectNotExistsAsync(yearPath, {
            type: "state",
            common: { name: `Total energy ${year}`, type: "number", role: "value.power.consumption", read: true, write: false, unit: "kWh" },
            native: {},
          }).then(() => this.safeSetState(yearPath, total, true))
        );
      }

      await Promise.all(promises);
    } catch (error) {
      this.log.error(`Error setting session data: ${this.getErrorMessage(error)}`);
    }
  }

  /**
   * Helper: Extract error message from various error types
   * @param {Error | string | Object} error The error object or string to parse
   */
  getErrorMessage(error) {
    if (!error) return "Unknown error";
    if (typeof error === "string") return error;
    if (error.response?.data?.message) return error.response.data.message;
    if (error instanceof Error) return error.message;
    if (error.message) return error.message;
    return String(error);
  }
}

if (require.main !== module) {
  // Export the constructor in compact mode
  module.exports = (options) => new Easee(options);
} else {
  // Otherwise start the instance directly
  new Easee();
}
