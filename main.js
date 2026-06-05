"use strict";

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

// Instance state (will be moved to class instance for better encapsulation)
const adapterIntervals = {};

class Easee extends utils.Adapter {
  constructor(options) {
    super({
      ...options,
      name: "easee",
    });
    this.on("ready", this.onReady.bind(this));
    this.on("stateChange", this.onStateChange.bind(this));
    this.on("unload", this.onUnload.bind(this));

    // Instance variables (moved from module-level for better encapsulation)
    this.accessToken = "";
    this.refreshToken = "";
    this.expireTime = Date.now();
    this.polltime = 300;
    this.logtype = false;
    this.roundCounter = 0;
    this.arrCharger = [];
    this.dynamicCircuitCurrentP = [0, 0, 0]; // P1, P2, P3

    // SignalR state
    this.signalRUnloaded = false;
    this.signalRBackoffMs = 1000;
    this.signalRReconnectTimer = undefined;
    this.signalRWatchdog = undefined;
    this.signalConnection = undefined;
    this.lastSignalRActivity = undefined;

    // Configure axios with HTTPS certificate validation
    this.configureAxios();
  }

  /**
   * Configure axios with secure defaults
   */
  configureAxios() {
    const httpsAgent = new https.Agent({
      rejectUnauthorized: true,
      minVersion: "TLSv1.2",
    });
    axios.defaults.httpsAgent = httpsAgent;
  }

  /**
   * Validate charger ID format
   * @param {string} chargerId - The charger ID to validate
   */
  validateChargerId(chargerId) {
    if (!chargerId || typeof chargerId !== "string" || chargerId.trim() === "") {
      throw new Error("Invalid charger ID: must be a non-empty string");
    }
    return chargerId.trim();
  }

  /**
   * Validate site ID format
   * @param {string} siteId - The site ID to validate
   */
  validateSiteId(siteId) {
    if (!siteId || typeof siteId !== "string" || siteId.trim() === "") {
      throw new Error("Invalid site ID: must be a non-empty string");
    }
    return siteId.trim();
  }

  /**
   * Validate circuit ID format
   * @param {string} circuitId - The circuit ID to validate
   */
  validateCircuitId(circuitId) {
    if (!circuitId || typeof circuitId !== "string" || circuitId.trim() === "") {
      throw new Error("Invalid circuit ID: must be a non-empty string");
    }
    return circuitId.trim();
  }

  /**
   * Start SignalR connection with error handling and watchdog
   */
  startSignal() {
    // Bail out if the adapter is shutting down
    if (this.signalRUnloaded) return;

    const connection = new signalR.HubConnectionBuilder()
      .withUrl(SIGNAL_R_URL, {
        accessTokenFactory: () => this.accessToken,
        skipNegotiation: true,
        transport: signalR.HttpTransportType.WebSockets,
      })
      .withAutomaticReconnect()
      .build();

    this.signalConnection = connection;
    this.lastSignalRActivity = Date.now();

    connection.on("ProductUpdate", (data) => {
      this.handleSignalRProductUpdate(data);
    });

    connection
      .start()
      .then(() => {
        this.signalRBackoffMs = 1000;
        this.subscribeAllChargersToSignalR(connection);
      })
      .catch((err) => {
        this.log.warn(
          `SignalR start() failed: ${this.getErrorMessage(err)}`
        );
      });

    connection.onclose(() => {
      if (this.signalRUnloaded) return;
      this.handleSignalRDisconnection();
    });

    this.startSignalRWatchdog();
  }

  /**
   * Handle ProductUpdate event from SignalR
   * @param {Object} data - The product update data from SignalR
   */
  handleSignalRProductUpdate(data) {
    // Update heartbeat first
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

    const tmpValueId = data.mid + dataName;
    const convertedValue = this.convertSignalRValue(data.value, data.dataType);

    this.log.debug(
      `New value over SignalR for: ${tmpValueId}, value: ${convertedValue}`
    );
    this.setStateAsync(tmpValueId, { val: convertedValue, ack: true }).catch(
      (err) => {
        this.log.error(`Failed to set state ${tmpValueId}: ${this.getErrorMessage(err)}`);
      }
    );
  }

  /**
   * Convert SignalR value based on dataType
   * @param {*} value - The value to convert
   * @param {number} dataType - The data type code (2=boolean, 3=float, 4=integer)
   */
  convertSignalRValue(value, dataType) {
    switch (dataType) {
      case 2: // Boolean
        return value === "1";
      case 3: // Float
        return parseFloat(value);
      case 4: // Integer
        return parseInt(value, 10);
      default:
        return value;
    }
  }

  /**
   * Subscribe all known chargers to SignalR
   * @param {Object} connection - The SignalR connection instance
   */
  subscribeAllChargersToSignalR(connection) {
    this.arrCharger.forEach((chargerId) => {
      connection
        .send("SubscribeWithCurrentState", chargerId, true)
        .then(() => {
          this.log.info(`Charger registered in SignalR: ${chargerId}`);
        })
        .catch((err) => {
          this.log.warn(
            `SignalR subscribe for ${chargerId} failed: ${this.getErrorMessage(err)}`
          );
        });
    });
  }

  /**
   * Handle SignalR disconnection with exponential backoff
   */
  handleSignalRDisconnection() {
    if (this.signalRUnloaded) return;

    const delay = this.signalRBackoffMs || 1000;
    this.log.error(
      `SignalR connection closed – restart in ${Math.round(delay / 1000)}s`
    );

    this.signalRBackoffMs = Math.min(
      (this.signalRBackoffMs || 1000) * 2,
      60000
    );

    this.signalRReconnectTimer = setTimeout(() => {
      this.signalRReconnectTimer = undefined;
      this.startSignal();
    }, delay);
  }

  /**
   * Start SignalR silent-zombie watchdog
   */
  startSignalRWatchdog() {
    if (this.signalRWatchdog) return;

    this.signalRWatchdog = setInterval(() => {
      if (this.signalRUnloaded) return;

      const silenceMs = Date.now() - (this.lastSignalRActivity || 0);
      if (silenceMs > 120000) {
        this.log.warn(
          `SignalR silent for ${Math.round(silenceMs / 1000)}s, forcing reconnect`
        );
        this.lastSignalRActivity = Date.now();

        if (this.signalConnection) {
          this.signalConnection.stop().catch(() => {
            /* ignore */
          });
        }
      }
    }, 60000);
  }

  /**
   * Initialize the adapter
   */
  async onReady() {
    try {
      await this.setStateAsync("info.connection", false, true);

      // Validate and load configuration
      this.validateConfiguration();

      // Initialize adapter
      await this.initializeAdapter();
    } catch (error) {
      this.log.error(
        `onReady failed: ${this.getErrorMessage(error)}`
      );
      await this.setStateAsync("info.connection", false, true);
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

    // Validate and set poll time
    if (this.config.polltime < 1) {
      this.log.warn(
        "Poll interval too short, using default 300 seconds"
      );
      this.polltime = 300;
    } else {
      this.polltime = this.config.polltime;
    }

    this.logtype = this.config.logtype || false;
  }

  /**
   * Initialize adapter after successful configuration
   */
  async initializeAdapter() {
    this.log.debug("Starting adapter initialization");

    // Attempt login
    const loginSuccess = await this.login(
      this.config.username,
      this.config.client_secret
    );

    if (!loginSuccess) {
      throw new Error("Login failed");
    }

    // Create lastUpdate object
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

    // Reset charger list
    this.arrCharger.length = 0;

    // Start polling
    await this.readAllStates();

    // Start SignalR if enabled
    if (this.config.signalR) {
      this.log.info("Starting SignalR connection");
      this.startSignal();
    }
  }

  /**
   * Clean up and unload the adapter
   * @param {Function} callback - The callback function to invoke when unload is complete
   */
  onUnload(callback) {
    try {
      // Clear all intervals and timers
      if (adapterIntervals.readAllStates) {
        clearTimeout(adapterIntervals.readAllStates);
      }
      if (adapterIntervals.updateDynamicCircuitCurrent) {
        clearTimeout(adapterIntervals.updateDynamicCircuitCurrent);
      }

      // Stop SignalR
      this.signalRUnloaded = true;

      if (this.signalRReconnectTimer) {
        clearTimeout(this.signalRReconnectTimer);
        this.signalRReconnectTimer = undefined;
      }

      if (this.signalRWatchdog) {
        clearInterval(this.signalRWatchdog);
        this.signalRWatchdog = undefined;
      }

      if (this.signalConnection) {
        this.signalConnection.stop().catch(() => {
          /* ignore */
        });
        this.signalConnection = undefined;
      }

      // Clear sensitive data
      this.accessToken = "";
      this.refreshToken = "";
      this.expireTime = Date.now();

      this.log.info("Adapter cleanup completed");

      this.setStateAsync("info.connection", false, true)
        .then(() => callback())
        .catch((err) => {
          this.log.error(`Error during shutdown: ${this.getErrorMessage(err)}`);
          callback();
        });
    } catch (error) {
      this.log.error(
        `Error during unload: ${this.getErrorMessage(error)}`
      );
      callback();
    }
  }

  /**
   * Main polling loop to read all charger states
   */
  async readAllStates() {
    try {
      // Check and refresh token if needed
      if (this.expireTime <= Date.now()) {
        this.log.debug("Token expired, refreshing");
        const refreshSuccess = await this.renewToken();
        if (!refreshSuccess) {
          this.log.warn("Token renew failed, will retry on next cycle");
          return;
        }
      }

      this.log.debug("Reading all states from API");

      // Get all chargers
      const chargers = await this.getAllCharger();
      if (!chargers || !Array.isArray(chargers)) {
        this.log.warn("No chargers found or invalid response from API");
        return;
      }

      // Process each charger sequentially
      for (const charger of chargers) {
        try {
          await this.processCharger(charger);
        } catch (error) {
          this.log.error(
            `Error processing charger ${charger?.id}: ${this.getErrorMessage(error)}`
          );
        }
      }

      // Manage energy data polling (throttled)
      if (
        this.roundCounter >
        MIN_POLL_TIME_ENERGY / this.polltime
      ) {
        this.roundCounter = 0;
      }
      this.roundCounter++;

      // Update last sync time
      await this.setStateAsync(
        "lastUpdate",
        new Date().toLocaleTimeString(),
        true
      );
    } catch (error) {
      this.log.error(
        `readAllStates failed: ${this.getErrorMessage(error)}`
      );
    } finally {
      // Always schedule next polling cycle
      adapterIntervals.readAllStates = setTimeout(
        this.readAllStates.bind(this),
        this.polltime * 1000
      );
    }
  }

  /**
   * Process a single charger
   * @param {Object} charger - The charger object to process
   */
  async processCharger(charger) {
    if (!charger || !charger.id) {
      throw new Error("Invalid charger object");
    }

    const chargerId = this.validateChargerId(charger.id);

    // Initialize charger if not known
    if (!this.arrCharger.includes(chargerId)) {
      await this.setAllStatusObjects(charger);
      await this.setAllConfigObjects(charger);
      this.arrCharger.push(chargerId);
      this.log.debug(`Initialized new charger: ${chargerId}`);
    }

    // Get charger state and config
    const [chargerState, chargerConfig] = await Promise.all([
      this.getChargerState(chargerId),
      this.getChargerConfig(chargerId),
    ]);

    // Update states
    await this.setNewStatusToCharger(charger, chargerState);
    await this.setConfigStatus(charger, chargerConfig);

    // Get energy data (throttled)
    if (
      this.roundCounter >
      MIN_POLL_TIME_ENERGY / this.polltime
    ) {
      const chargerSession = await this.getChargerSession(chargerId);
      await this.setNewSessionToCharger(charger, chargerSession);
    }
  }

  /**
   * Handle state change events
   * @param {string} id - The state ID that changed
   * @param {Object} state - The new state object
   */
  onStateChange(id, state) {
    if (!state) {
      this.log.info(`State deleted: ${id}`);
      return;
    }

    this.log.debug(`State changed: ${id} = ${state.val} (ack=${state.ack})`);

    try {
      const parts = id.split(".");
      if (parts.length < 5) {
        this.log.warn(`Invalid state ID format: ${id}`);
        return;
      }

      const chargerId = parts[2];
      const category = parts[3];
      const property = parts[4];

      if (category === "config") {
        this.handleConfigChange(chargerId, property, state);
      } else if (category === "control") {
        this.handleControlChange(chargerId, property);
      }
    } catch (error) {
      this.log.error(
        `Error handling state change ${id}: ${this.getErrorMessage(error)}`
      );
    }
  }

  /**
   * Handle configuration change
   * @param {string} chargerId - The charger ID
   * @param {string} property - The property name being changed
   * @param {Object} state - The new state object
   */
  async handleConfigChange(chargerId, property, state) {
    try {
      if (state.ack) {
        // Ignore acknowledged changes
        return;
      }

      chargerId = this.validateChargerId(chargerId);

      if (
        property === "circuitMaxCurrentP1" ||
        property === "circuitMaxCurrentP2" ||
        property === "circuitMaxCurrentP3"
      ) {
        await this.handleCircuitMaxCurrentChange(
          chargerId,
          property,
          state.val
        );
      } else if (
        property === "dynamicCircuitCurrentP1" ||
        property === "dynamicCircuitCurrentP2" ||
        property === "dynamicCircuitCurrentP3"
      ) {
        await this.handleDynamicCircuitCurrentChange(
          chargerId,
          property,
          state.val
        );
      } else if (property === "isEnabled") {
        await this.changeConfig(chargerId, "enabled", state.val);
      } else {
        await this.changeConfig(chargerId, property, state.val);
      }
    } catch (error) {
      this.log.error(
        `Error handling config change ${chargerId}.${property}: ${this.getErrorMessage(error)}`
      );
    }
  }

  /**
   * Handle circuit max current change
   * @param {string} chargerId - The charger ID
   * @param {string} property - The property name
   * @param {number} value - The new current value in amperes
   */
  async handleCircuitMaxCurrentChange(chargerId, property, value) {
    try {
      const site = await this.getChargerSite(chargerId);

      if (!site || !site.circuits || site.circuits.length === 0) {
        throw new Error("Invalid site data: no circuits found");
      }

      this.log.debug(
        `Updating ${property} to ${value} for circuit ${site.circuits[0].id}`
      );
      await this.changeMaxCircuitConfig(
        site.id,
        site.circuits[0].id,
        value
      );
    } catch (error) {
      this.log.error(
        `Failed to update circuit max current: ${this.getErrorMessage(error)}`
      );
    }
  }

  /**
   * Handle dynamic circuit current change with debouncing
   * @param {string} chargerId - The charger ID
   * @param {string} property - The property name (dynamicCircuitCurrentP1/P2/P3)
   * @param {number} value - The new current value in amperes
   */
  async handleDynamicCircuitCurrentChange(
    chargerId,
    property,
    value
  ) {
    try {
      const phaseIndex =
        property === "dynamicCircuitCurrentP1"
          ? 0
          : property === "dynamicCircuitCurrentP2"
            ? 1
            : 2;

      this.dynamicCircuitCurrentP[phaseIndex] = Number(value);

      // Clear existing timer
      if (adapterIntervals.updateDynamicCircuitCurrent) {
        clearTimeout(adapterIntervals.updateDynamicCircuitCurrent);
      }

      // Get site info once
      const site = await this.getChargerSite(chargerId);

      if (!site || !site.circuits || site.circuits.length === 0) {
        throw new Error("Invalid site data: no circuits found");
      }

      // Debounce the update by 500ms to collect all phase changes
      adapterIntervals.updateDynamicCircuitCurrent = setTimeout(
        async () => {
          try {
            this.log.debug(
              `Updating dynamic circuit current: P1=${this.dynamicCircuitCurrentP[0]}, P2=${this.dynamicCircuitCurrentP[1]}, P3=${this.dynamicCircuitCurrentP[2]}`
            );
            await this.changeCircuitConfig(
              site.id,
              site.circuits[0].id
            );
          } catch (error) {
            this.log.error(
              `Failed to update dynamic circuit current: ${this.getErrorMessage(error)}`
            );
          } finally {
            adapterIntervals.updateDynamicCircuitCurrent = undefined;
          }
        },
        500
      );
    } catch (error) {
      this.log.error(
        `Error handling dynamic circuit current change: ${this.getErrorMessage(error)}`
      );
    }
  }

  /**
   * Handle control commands (start, stop, pause, resume, reboot)
   * @param {string} chargerId - The charger ID
   * @param {string} command - The command to execute
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

      this.log.info(`Executing control: ${command} on charger ${chargerId}`);
      await handler();
    } catch (error) {
      this.log.error(
        `Error handling control command ${command}: ${this.getErrorMessage(error)}`
      );
    }
  }

  /**
   * Set charger status values
   * @param {Object} charger - The charger object
   * @param {Object} chargerStates - The charger state data from API
   */
  async setNewStatusToCharger(charger, chargerStates) {
    try {
      const baseId = charger.id;
      const stateUpdates = [
        [`${baseId}.name`, charger.name],
        [`${baseId}.status.cableLocked`, chargerStates.cableLocked],
        [`${baseId}.status.chargerOpMode`, chargerStates.chargerOpMode],
        [`${baseId}.status.totalPower`, chargerStates.totalPower],
        [`${baseId}.status.wiFiRSSI`, chargerStates.wiFiRSSI],
        [`${baseId}.status.chargerFirmware`, chargerStates.chargerFirmware],
        [
          `${baseId}.status.reasonForNoCurrent`,
          chargerStates.reasonForNoCurrent,
        ],
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
        [
          `${baseId}.config.dynamicChargerCurrent`,
          { val: chargerStates.dynamicChargerCurrent, ack: true },
        ],
        [
          `${baseId}.config.dynamicCircuitCurrentP1`,
          { val: chargerStates.dynamicCircuitCurrentP1, ack: true },
        ],
        [
          `${baseId}.config.dynamicCircuitCurrentP2`,
          { val: chargerStates.dynamicCircuitCurrentP2, ack: true },
        ],
        [
          `${baseId}.config.dynamicCircuitCurrentP3`,
          { val: chargerStates.dynamicCircuitCurrentP3, ack: true },
        ],
        [`${baseId}.config.smartCharging`, chargerStates.smartCharging],
      ];

      // Use Promise.all for concurrent updates
      await Promise.all(
        stateUpdates.map(([id, value]) =>
          this.setStateAsync(id, value, true).catch((err) => {
            this.log.warn(
              `Failed to set state ${id}: ${this.getErrorMessage(err)}`
            );
          })
        )
      );
    } catch (error) {
      this.log.error(
        `Error setting charger status: ${this.getErrorMessage(error)}`
      );
    }
  }

  /**
   * Set charger configuration values
   * @param {Object} charger - The charger object
   * @param {Object} chargerConfig - The charger configuration data from API
   */
  async setConfigStatus(charger, chargerConfig) {
    try {
      const baseId = charger.id;
      const stateUpdates = [
        [
          `${baseId}.config.isEnabled`,
          { val: chargerConfig.isEnabled, ack: true },
        ],
        [
          `${baseId}.config.phaseMode`,
          { val: chargerConfig.phaseMode, ack: true },
        ],
        [
          `${baseId}.config.ledStripBrightness`,
          { val: chargerConfig.ledStripBrightness, ack: true },
        ],
        [
          `${baseId}.config.smartButtonEnabled`,
          { val: chargerConfig.smartButtonEnabled, ack: true },
        ],
        [
          `${baseId}.config.wiFiSSID`,
          { val: chargerConfig.wiFiSSID, ack: true },
        ],
        [
          `${baseId}.config.maxChargerCurrent`,
          { val: chargerConfig.maxChargerCurrent, ack: true },
        ],
        [
          `${baseId}.config.circuitMaxCurrentP1`,
          { val: chargerConfig.circuitMaxCurrentP1, ack: true },
        ],
        [
          `${baseId}.config.circuitMaxCurrentP2`,
          { val: chargerConfig.circuitMaxCurrentP2, ack: true },
        ],
        [
          `${baseId}.config.circuitMaxCurrentP3`,
          { val: chargerConfig.circuitMaxCurrentP3, ack: true },
        ],
      ];

      await Promise.all(
        stateUpdates.map(([id, value]) =>
          this.setStateAsync(id, value).catch((err) => {
            this.log.warn(
              `Failed to set config state ${id}: ${this.getErrorMessage(err)}`
            );
          })
        )
      );
    } catch (error) {
      this.log.error(
        `Error setting charger config: ${this.getErrorMessage(error)}`
      );
    }
  }

  /**
   * API: Login and get access token
   * @param {string} username - The username for authentication
   * @param {string} password - The password for authentication
   */
  async login(username, password) {
    try {
      if (!username || !password) {
        throw new Error("Username and password required");
      }

      this.log.debug("Attempting login");

      const response = await axios.post(`${API_URL}/api/accounts/login`, {
        userName: username,
        password: password,
      });

      this.accessToken = response.data.accessToken;
      this.refreshToken = response.data.refreshToken;
      this.expireTime =
        Date.now() + (response.data.expiresIn * 1000 - TOKEN_SAFETY_MARGIN);

      this.log.info("Login successful");
      this.log.debug(
        `Token expires in ${response.data.expiresIn}s (${Math.round((this.expireTime - Date.now()) / 1000)}s remaining)`
      );

      await this.setStateAsync("info.connection", true, true);
      return true;
    } catch (error) {
      this.log.error(
        `Login failed: ${this.getErrorMessage(error)}`
      );
      await this.setStateAsync("info.connection", false, true);
      return false;
    }
  }

  /**
   * API: Refresh access token
   */
  async renewToken() {
    try {
      this.log.debug("Refreshing token");

      const response = await axios.post(`${API_URL}/api/accounts/refresh_token`, {
        accessToken: this.accessToken,
        refreshToken: this.refreshToken,
      });

      this.accessToken = response.data.accessToken;
      this.refreshToken = response.data.refreshToken;
      this.expireTime =
        Date.now() + (response.data.expiresIn * 1000 - TOKEN_SAFETY_MARGIN);

      if (this.logtype) {
        this.log.info("Token refreshed successfully");
      }

      await this.setStateAsync("info.connection", true, true);
      return true;
    } catch (error) {
      const status = error?.response?.status;

      this.log.warn(
        `Token refresh failed (HTTP ${status || "?"}): ${this.getErrorMessage(error)}`
      );

      // On 4xx errors, fall back to full login
      if (status >= 400 && status < 500) {
        this.log.info("Refresh token invalid, attempting full login");
        const loginSuccess = await this.login(
          this.config.username,
          this.config.client_secret
        );
        if (loginSuccess) return true;
        this.log.error("Full login also failed after refresh token error");
      }

      this.expireTime = Date.now(); // Force re-authentication on next cycle
      await this.setStateAsync("info.connection", false, true);
      return false;
    }
  }

  /**
   * Helper: GET with proper error handling and retry on 5xx
   * @param {string} path - The API path to request
   * @param {string} context - The context string for logging
   */
  async _apiGet(path, context) {
    const url = API_URL + path;
    const config = {
      headers: {
        Authorization: `Bearer ${this.accessToken}`,
      },
    };

    for (let attempt = 1; attempt <= 2; attempt++) {
      try {
        const response = await axios.get(url, config);
        this.log.debug(`${context}: success`);
        return response.data;
      } catch (error) {
        const status = error?.response?.status;

        if (status === 401) {
          // Token expired, force refresh on next cycle
          this.expireTime = Date.now();
          throw new Error(
            `${context}: HTTP 401 - Token rejected`
          );
        }

        if (status === 429) {
          // Rate limited, don't retry
          this.log.warn(`${context}: HTTP 429 - Rate limited`);
          throw new Error(`${context}: Rate limited`);
        }

        if (status >= 500 && status < 600 && attempt === 1) {
          // Server error, retry once
          this.log.warn(
            `${context}: HTTP ${status}, retrying in 1s`
          );
          await new Promise((r) => setTimeout(r, 1000));
          continue;
        }

        const msg = this.getErrorMessage(error);
        throw new Error(
          `${context}: ${status ? `HTTP ${status}` : "request failed"} - ${msg}`
        );
      }
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
   * @param {string} chargerId - The charger ID
   */
  async getChargerState(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    return await this._apiGet(
      `/api/chargers/${chargerId}/state`,
      `getChargerState(${chargerId})`
    );
  }

  /**
   * Get charger configuration
   * @param {string} chargerId - The charger ID
   */
  async getChargerConfig(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    return await this._apiGet(
      `/api/chargers/${chargerId}/config`,
      `getChargerConfig(${chargerId})`
    );
  }

  /**
   * Get charger site information
   * @param {string} chargerId - The charger ID
   */
  async getChargerSite(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    return await this._apiGet(
      `/api/chargers/${chargerId}/site`,
      `getChargerSite(${chargerId})`
    );
  }

  /**
   * Get charger session data
   * @param {string} chargerId - The charger ID
   */
  async getChargerSession(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    return await this._apiGet(
      `/api/sessions/charger/${chargerId}/monthly`,
      `getChargerSession(${chargerId})`
    );
  }

  /**
   * Start charging
   * @param {string} chargerId - The charger ID
   */
  async startCharging(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    try {
      await axios.post(
        `${API_URL}/api/chargers/${chargerId}/commands/start_charging`,
        {},
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
          },
        }
      );
      this.log.info(`Start charging successful for ${chargerId}`);
    } catch (error) {
      this.log.error(
        `Start charging failed for ${chargerId}: ${this.getErrorMessage(error)}`
      );
      throw error;
    }
  }

  /**
   * Stop charging
   * @param {string} chargerId - The charger ID
   */
  async stopCharging(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    try {
      await axios.post(
        `${API_URL}/api/chargers/${chargerId}/commands/stop_charging`,
        {},
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
          },
        }
      );
      this.log.info(`Stop charging successful for ${chargerId}`);
    } catch (error) {
      this.log.error(
        `Stop charging failed for ${chargerId}: ${this.getErrorMessage(error)}`
      );
      throw error;
    }
  }

  /**
   * Pause charging
   * @param {string} chargerId - The charger ID
   */
  async pauseCharging(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    try {
      await axios.post(
        `${API_URL}/api/chargers/${chargerId}/commands/pause_charging`,
        {},
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
          },
        }
      );
      this.log.info(`Pause charging successful for ${chargerId}`);
    } catch (error) {
      this.log.error(
        `Pause charging failed for ${chargerId}: ${this.getErrorMessage(error)}`
      );
      throw error;
    }
  }

  /**
   * Resume charging
   * @param {string} chargerId - The charger ID
   */
  async resumeCharging(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    try {
      await axios.post(
        `${API_URL}/api/chargers/${chargerId}/commands/resume_charging`,
        {},
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
          },
        }
      );
      this.log.info(`Resume charging successful for ${chargerId}`);
    } catch (error) {
      this.log.error(
        `Resume charging failed for ${chargerId}: ${this.getErrorMessage(error)}`
      );
      throw error;
    }
  }

  /**
   * Reboot charger
   * @param {string} chargerId - The charger ID
   */
  async rebootCharging(chargerId) {
    chargerId = this.validateChargerId(chargerId);
    try {
      await axios.post(
        `${API_URL}/api/chargers/${chargerId}/commands/reboot`,
        {},
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
          },
        }
      );
      this.log.info(`Reboot successful for ${chargerId}`);
    } catch (error) {
      this.log.error(
        `Reboot failed for ${chargerId}: ${this.getErrorMessage(error)}`
      );
      throw error;
    }
  }

  /**
   * Change charger configuration
   * @param {string} chargerId - The charger ID
   * @param {string} configKey - The configuration key to update
   * @param {*} value - The new configuration value
   */
  async changeConfig(chargerId, configKey, value) {
    chargerId = this.validateChargerId(chargerId);
    try {
      this.log.debug(
        `Updating charger config: ${configKey} = ${value}`
      );

      await axios.post(
        `${API_URL}/api/chargers/${chargerId}/settings`,
        {
          [configKey]: value,
        },
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
          },
        }
      );

      this.log.info(
        `Config update successful: ${configKey} = ${value}`
      );
    } catch (error) {
      this.log.error(
        `Config update failed: ${this.getErrorMessage(error)}`
      );
      throw error;
    }
  }

  /**
   * Change circuit maximum current
   * @param {string} siteId - The site ID
   * @param {string} circuitId - The circuit ID
   * @param {number} value - The maximum current value in amperes
   */
  async changeMaxCircuitConfig(siteId, circuitId, value) {
    siteId = this.validateSiteId(siteId);
    circuitId = this.validateCircuitId(circuitId);

    try {
      this.log.debug(
        `Updating circuit max current to ${value}`
      );

      await axios.post(
        `${API_URL}/api/sites/${siteId}/circuits/${circuitId}/settings`,
        {
          maxCircuitCurrentP1: value,
          maxCircuitCurrentP2: value,
          maxCircuitCurrentP3: value,
        },
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
          },
        }
      );

      this.log.info(
        `Circuit max current update successful: ${value}A`
      );
    } catch (error) {
      this.log.error(
        `Circuit max current update failed: ${this.getErrorMessage(error)}`
      );
      throw error;
    }
  }

  /**
   * Change dynamic circuit current
   * @param {string} siteId - The site ID
   * @param {string} circuitId - The circuit ID
   */
  async changeCircuitConfig(siteId, circuitId) {
    siteId = this.validateSiteId(siteId);
    circuitId = this.validateCircuitId(circuitId);

    try {
      this.log.debug(
        `Updating dynamic circuit current: P1=${this.dynamicCircuitCurrentP[0]}, P2=${this.dynamicCircuitCurrentP[1]}, P3=${this.dynamicCircuitCurrentP[2]}`
      );

      await axios.post(
        `${API_URL}/api/sites/${siteId}/circuits/${circuitId}/settings`,
        {
          dynamicCircuitCurrentP1: this.dynamicCircuitCurrentP[0],
          dynamicCircuitCurrentP2: this.dynamicCircuitCurrentP[1],
          dynamicCircuitCurrentP3: this.dynamicCircuitCurrentP[2],
        },
        {
          headers: {
            Authorization: `Bearer ${this.accessToken}`,
          },
        }
      );

      this.log.info("Dynamic circuit current update successful");

      // Reset values
      this.dynamicCircuitCurrentP = [0, 0, 0];
    } catch (error) {
      this.log.error(
        `Dynamic circuit current update failed: ${this.getErrorMessage(error)}`
      );
      throw error;
    }
  }

  /**
   * Create status objects for a charger
   * @param {Object} charger - The charger object
   */
  async setAllStatusObjects(charger) {
    try {
      const baseId = charger.id;

      // Control buttons
      const controlButtons = [
        { name: "start", displayName: "Start charging" },
        { name: "stop", displayName: "Stop charging" },
        { name: "pause", displayName: "Pause charging" },
        { name: "resume", displayName: "Resume charging" },
        { name: "reboot", displayName: "Reboot Charger" },
      ];

      for (const button of controlButtons) {
        await this.setObjectNotExistsAsync(`${baseId}.control.${button.name}`, {
          type: "state",
          common: {
            name: button.displayName,
            type: "boolean",
            role: "button",
            read: false,
            write: true,
          },
          native: {},
        });
        this.subscribeStates(`${baseId}.control.${button.name}`);
      }

      // ID and Name
      await this.setObjectNotExistsAsync(`${baseId}.id`, {
        type: "state",
        common: {
          name: "Charger ID",
          type: "string",
          role: "info.name",
          read: true,
          write: false,
        },
        native: {},
      });
      await this.setStateAsync(`${baseId}.id`, charger.id, true);

      await this.setObjectNotExistsAsync(`${baseId}.name`, {
        type: "state",
        common: {
          name: "Charger Name",
          type: "string",
          role: "info.name",
          read: true,
          write: false,
        },
        native: {},
      });

      // Status objects
      const statusObjects = [
        {
          name: "cableLocked",
          displayName: "Cable lock state",
          type: "boolean",
          role: "sensor.lock",
        },
        {
          name: "chargerOpMode",
          displayName: "Charger operation mode",
          type: "number",
          role: "value",
          states: {
            0: "Offline",
            1: "Disconnected",
            2: "AwaitingStart",
            3: "Charging",
            4: "Completed",
            5: "Error",
            6: "ReadyToCharge",
            7: "AwaitingAuthentication",
            8: "DeAuthenticating",
          },
        },
        {
          name: "totalPower",
          displayName: "Total power",
          type: "number",
          role: "value.power",
          unit: "kW",
        },
        {
          name: "wiFiRSSI",
          displayName: "WiFi signal strength",
          type: "number",
          role: "value",
          unit: "dBm",
        },
        {
          name: "chargerFirmware",
          displayName: "Modem firmware version",
          type: "number",
          role: "info.firmware",
        },
        {
          name: "reasonForNoCurrent",
          displayName: "Reason for no current",
          type: "number",
          role: "value",
        },
        {
          name: "voltage",
          displayName: "Voltage",
          type: "number",
          role: "value.voltage",
          unit: "V",
        },
        {
          name: "outputCurrent",
          displayName: "Output current",
          type: "number",
          role: "value.current",
          unit: "A",
        },
        {
          name: "isOnline",
          displayName: "Charger online",
          type: "boolean",
          role: "indicator.reachable",
        },
        {
          name: "wiFiAPEnabled",
          displayName: "WiFi AP enabled",
          type: "boolean",
          role: "indicator",
        },
        {
          name: "ledMode",
          displayName: "LED mode",
          type: "number",
          role: "value",
        },
        {
          name: "lifetimeEnergy",
          displayName: "Lifetime energy",
          type: "number",
          role: "value.power.consumption",
          unit: "kWh",
        },
        {
          name: "energyPerHour",
          displayName: "Energy per hour",
          type: "number",
          role: "value.power.consumption",
          unit: "kWh",
        },
      ];

      for (const obj of statusObjects) {
        const common = {
          name: obj.displayName,
          type: obj.type,
          role: obj.role,
          read: true,
          write: false,
        };

        if (obj.unit) common.unit = obj.unit;
        if (obj.states) common.states = obj.states;

        await this.setObjectNotExistsAsync(`${baseId}.status.${obj.name}`, {
          type: "state",
          common,
          native: {},
        });
      }

      // Current inputs
      for (let i = 2; i <= 5; i++) {
        await this.setObjectNotExistsAsync(`${baseId}.status.inCurrentT${i}`, {
          type: "state",
          common: {
            name: `Current RMS input T${i}`,
            type: "number",
            role: "value.current",
            read: true,
            write: false,
            unit: "A",
          },
          native: {},
        });
      }

      // Voltage pairs
      const voltagePairs = [
        [1, 2],
        [1, 3],
        [1, 4],
        [1, 5],
        [2, 3],
        [2, 4],
        [2, 5],
        [3, 4],
        [3, 5],
        [4, 5],
      ];

      for (const [t1, t2] of voltagePairs) {
        await this.setObjectNotExistsAsync(
          `${baseId}.status.inVoltageT${t1}T${t2}`,
          {
            type: "state",
            common: {
              name: `Voltage between T${t1} and T${t2}`,
              type: "number",
              role: "value.voltage",
              read: true,
              write: false,
              unit: "V",
            },
            native: {},
          }
        );
      }

      // SignalR only
      await this.setObjectNotExistsAsync(`${baseId}.status.TempMax`, {
        type: "state",
        common: {
          name: "Maximum temperature (SignalR only)",
          type: "number",
          role: "value.temperature.max",
          read: true,
          write: false,
        },
        native: {},
      });
    } catch (error) {
      this.log.error(
        `Error creating status objects: ${this.getErrorMessage(error)}`
      );
    }
  }

  /**
   * Create configuration objects for a charger
   * @param {Object} charger - The charger object
   */
  async setAllConfigObjects(charger) {
    try {
      const baseId = charger.id;

      const configObjects = [
        {
          name: "isEnabled",
          displayName: "Charger enabled",
          type: "boolean",
          role: "switch.enabled",
        },
        {
          name: "phaseMode",
          displayName: "Phase mode",
          type: "number",
          role: "level",
        },
        {
          name: "maxChargerCurrent",
          displayName: "Max charger current",
          type: "number",
          role: "level.current",
          unit: "A",
        },
        {
          name: "dynamicChargerCurrent",
          displayName: "Dynamic charger current",
          type: "number",
          role: "level.current",
          unit: "A",
        },
        {
          name: "dynamicCircuitCurrentP1",
          displayName: "Dynamic circuit current P1",
          type: "number",
          role: "level.current",
          unit: "A",
        },
        {
          name: "dynamicCircuitCurrentP2",
          displayName: "Dynamic circuit current P2",
          type: "number",
          role: "level.current",
          unit: "A",
        },
        {
          name: "dynamicCircuitCurrentP3",
          displayName: "Dynamic circuit current P3",
          type: "number",
          role: "level.current",
          unit: "A",
        },
        {
          name: "circuitMaxCurrentP1",
          displayName: "Circuit max current P1",
          type: "number",
          role: "level.current",
          unit: "A",
        },
        {
          name: "circuitMaxCurrentP2",
          displayName: "Circuit max current P2",
          type: "number",
          role: "level.current",
          unit: "A",
        },
        {
          name: "circuitMaxCurrentP3",
          displayName: "Circuit max current P3",
          type: "number",
          role: "level.current",
          unit: "A",
        },
        {
          name: "ledStripBrightness",
          displayName: "LED strip brightness",
          type: "number",
          role: "level.brightness",
        },
        {
          name: "smartCharging",
          displayName: "Smart charging enabled",
          type: "boolean",
          role: "switch.enable",
        },
        {
          name: "smartButtonEnabled",
          displayName: "Smart button enabled",
          type: "boolean",
          role: "switch.enable",
        },
        {
          name: "wiFiSSID",
          displayName: "WiFi SSID",
          type: "string",
          role: "text",
        },
      ];

      for (const obj of configObjects) {
        const common = {
          name: obj.displayName,
          type: obj.type,
          role: obj.role,
          read: true,
          write: true,
        };

        if (obj.unit) common.unit = obj.unit;

        await this.setObjectNotExistsAsync(`${baseId}.config.${obj.name}`, {
          type: "state",
          common,
          native: {},
        });

        this.subscribeStates(`${baseId}.config.${obj.name}`);
      }
    } catch (error) {
      this.log.error(
        `Error creating config objects: ${this.getErrorMessage(error)}`
      );
    }
  }

  /**
   * Set charger session data
   * @param {Object} charger - The charger object
   * @param {Array} chargerSessions - The charger session data array
   */
  async setNewSessionToCharger(charger, chargerSessions) {
    try {
      if (!Array.isArray(chargerSessions)) {
        this.log.warn("Invalid charger sessions data");
        return;
      }

      const baseId = charger.id;

      // Create session objects for each month
      for (const session of chargerSessions) {
        if (!session.year || !session.month) continue;

        const sessionPath = `${baseId}.session.${session.year}.${session.month}`;

        await this.setObjectNotExistsAsync(`${sessionPath}.totalEnergyUsage`, {
          type: "state",
          common: {
            name: "Total energy usage",
            type: "number",
            role: "value.power.consumption",
            read: true,
            write: false,
            unit: "kWh",
          },
          native: {},
        });
        await this.setStateAsync(
          `${sessionPath}.totalEnergyUsage`,
          session.totalEnergyUsage,
          true
        );

        await this.setObjectNotExistsAsync(`${sessionPath}.totalCost`, {
          type: "state",
          common: {
            name: "Total cost",
            type: "number",
            role: "value.money",
            read: true,
            write: false,
          },
          native: {},
        });
        await this.setStateAsync(
          `${sessionPath}.totalCost`,
          session.totalCost,
          true
        );
      }

      // Calculate yearly totals
      const yearTotals = {};
      for (const session of chargerSessions) {
        if (!session.year) continue;
        yearTotals[session.year] =
          (yearTotals[session.year] || 0) + (session.totalEnergyUsage || 0);
      }

      // Set yearly totals
      for (const [year, total] of Object.entries(yearTotals)) {
        const yearPath = `${baseId}.session.${year}.total_year`;
        await this.setObjectNotExistsAsync(yearPath, {
          type: "state",
          common: {
            name: `Total energy ${year}`,
            type: "number",
            role: "value.power.consumption",
            read: true,
            write: false,
            unit: "kWh",
          },
          native: {},
        });
        await this.setStateAsync(yearPath, total, true);
      }
    } catch (error) {
      this.log.error(
        `Error setting session data: ${this.getErrorMessage(error)}`
      );
    }
  }

  /**
   * Helper: Extract error message from various error types
   * @param {*} error - The error object to extract message from
   */
  getErrorMessage(error) {
    if (!error) return "Unknown error";
    if (typeof error === "string") return error;
    if (error instanceof Error) return error.message;
    if (error.response?.data?.message) return error.response.data.message;
    if (error.message) return error.message;
    return String(error);
  }
}

if (module.parent) {
  // Export the constructor in compact mode
  module.exports = (options) => new Easee(options);
} else {
  // otherwise start the instance directly
  new Easee();
}
