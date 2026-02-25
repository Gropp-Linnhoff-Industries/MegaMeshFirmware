
#include <Arduino.h>
#include <RadioLib.h>
#include <BLEDevice.h>
#include <BLEUtils.h>
#include <BLEServer.h>
#include <BLE2902.h>
#include <Preferences.h>
#include <mbedtls/aes.h>
#include <esp_system.h>
// ESP-IDF BT helpers for explicit discoverability
#include "esp_bt.h"
#include "esp_bt_main.h"
#include "esp_gap_bt_api.h"
#include "esp_bt_device.h"

// Note: use ESP-IDF GAP enums `ESP_BT_CONNECTABLE`, `ESP_BT_GENERAL_DISCOVERABLE`,
// `ESP_BT_NON_CONNECTABLE`, `ESP_BT_NON_DISCOVERABLE` for scan mode control.

// BLE GATT UART objects
BLEAdvertising *pAdvertising = nullptr;
BLECharacteristic *pTXChar = nullptr;
BLECharacteristic *pRXChar = nullptr;
// also expose a fixed, well-known NUS-like service so web apps can request it
BLECharacteristic *pTXCharBase = nullptr;
BLECharacteristic *pRXCharBase = nullptr;
Preferences prefs;

struct LoraConfig
{
    uint32_t magic;
    uint8_t deviceType;
    uint8_t csPin;
    uint8_t resetPin;
    uint8_t busyPin;
    uint8_t dioPin;
    float frequency;
    float bandwidth;
    uint8_t spreadingFactor;
    uint8_t codingRate;
    uint8_t syncWord;
    uint16_t preambleLength;
    float tcxoVoltage;
    bool useDio2AsRfSwitch;
    bool btEnabled;
    uint8_t pwr;
    uint8_t sclkPin;
    uint8_t misoPin;
    uint8_t mosiPin;
    uint8_t nssPin;
    uint8_t rstPin;
    uint8_t dio0Pin;
    uint8_t dio1Pin;
};

const uint32_t CFG_MAGIC = 0x4C4F5241; // 'LORA'
LoraConfig cfg;

bool configSaved = false;
bool radioInitialized = false;
bool setupModeActive = false;
bool setupSaveRequested = false;

// LoRa radio pointer (instantiate when initializing)
SX1262 *radioPtr = nullptr;
bool btActive = false;

// Mesh placeholders
uint16_t nodeId = 0; // derived from MAC
bool meshRunning = false;

// Advanced mesh state
bool meshEncryptionEnabled = false;
uint8_t meshKey[16] = {0x10, 0x32, 0x54, 0x76, 0x98, 0xBA, 0xDC, 0xFE, 0x22, 0x44, 0x66, 0x88, 0xAA, 0xCC, 0xEE, 0x00};
uint32_t meshTxCounter = 1;

struct MeshPeer
{
    uint16_t id;
    uint32_t lastSeenMs;
};

static const uint8_t MAX_PEERS = 16;
MeshPeer peers[MAX_PEERS];
uint8_t peerCount = 0;

// Weather station mode
bool weatherModeEnabled = false;
uint32_t weatherIntervalMs = 5000;
uint32_t lastWeatherTxMs = 0;

struct SensorDef
{
    uint8_t pin;
    bool analog;
};

struct WeatherPersistConfig
{
    uint32_t magic;
    uint8_t weatherMode;
    uint32_t weatherIntervalMs;
    uint8_t sensorCount;
    SensorDef sensors[6];
};

const uint32_t WX_MAGIC = 0x57585452; // 'WXTR'

static const uint8_t MAX_SENSORS = 6;
SensorDef sensors[MAX_SENSORS];
uint8_t sensorCount = 0;

// BLE RX line buffers for newline-terminated command parsing
String bleRxBuffer;
String bleRxBaseBuffer;

// forward declaration for BLE callback usage
void handleCommand(String raw, bool fromBT);
void sendSetupInfo();
void startConfigMode();
void broadcastDiscovery();
void sendPeerList();
void sendWeatherPacket();
void saveWeatherConfig();
void loadWeatherConfig();
static uint16_t crc16_ccitt(const uint8_t *data, size_t len);

void checkUser()
{

    // platz für User settings
}

void enableBluetoothVisible(const char *name)
{
    // ble gatt
    if (!pAdvertising)
    {
        uint64_t mac = ESP.getEfuseMac();
        uint16_t node = (uint16_t)(mac & 0xFFFF);
        uint32_t base = 0x6E400000 | (uint32_t)node;
        char svc[64], rx[64], tx[64];
        sprintf(svc, "%08X-B5A3-F393-E0A9-E50E24DCCA9E", base | 0x0001);
        sprintf(rx, "%08X-B5A3-F393-E0A9-E50E24DCCA9E", base | 0x0002);
        sprintf(tx, "%08X-B5A3-F393-E0A9-E50E24DCCA9E", base | 0x0003);

        String devName = String(name) + "-" + String(node, HEX);
        BLEDevice::init(devName.c_str());
        BLEServer *pServer = BLEDevice::createServer();
        class LocalServerCB : public BLEServerCallbacks
        {
        public:
            void onConnect(BLEServer *srv) override
            {
                (void)srv;
            }

            void onDisconnect(BLEServer *srv) override
            {
                if (srv)
                {
                    srv->startAdvertising();
                }
            }
        };
        pServer->setCallbacks(new LocalServerCB());
        BLEService *pService = pServer->createService(BLEUUID(svc));

        pRXChar = pService->createCharacteristic(BLEUUID(rx), BLECharacteristic::PROPERTY_WRITE);
        class LocalRXCB : public BLECharacteristicCallbacks
        {
        public:
            explicit LocalRXCB(String *targetBuffer) : _targetBuffer(targetBuffer) {}

            void onWrite(BLECharacteristic *c) override
            {
                auto value = c->getValue();
                String incoming = String(value.c_str());
                if (!incoming.length())
                    return;

                for (size_t idx = 0; idx < incoming.length(); ++idx)
                {
                    char ch = incoming[idx];
                    if (ch == '\r')
                        continue;

                    if (ch == '\n')
                    {
                        String line = *_targetBuffer;
                        line.trim();
                        _targetBuffer->remove(0);
                        if (line.length())
                        {
                            handleCommand(line, true);
                        }
                        continue;
                    }

                    *_targetBuffer += ch;
                }
            }

        private:
            String *_targetBuffer;
        };
        pRXChar->setCallbacks(new LocalRXCB(&bleRxBuffer));

        pTXChar = pService->createCharacteristic(BLEUUID(tx), BLECharacteristic::PROPERTY_NOTIFY);

        pTXChar->addDescriptor(new BLE2902());

        pService->start();
        // also create a fixed base service so browsers can request a stable UUID
        BLEService *pBase = pServer->createService(BLEUUID("6E400001-B5A3-F393-E0A9-E50E24DCCA9E"));
        pRXCharBase = pBase->createCharacteristic(BLEUUID("6E400002-B5A3-F393-E0A9-E50E24DCCA9E"), BLECharacteristic::PROPERTY_WRITE);
        pRXCharBase->setCallbacks(new LocalRXCB(&bleRxBaseBuffer));
        pTXCharBase = pBase->createCharacteristic(BLEUUID("6E400003-B5A3-F393-E0A9-E50E24DCCA9E"), BLECharacteristic::PROPERTY_NOTIFY);
        pTXCharBase->addDescriptor(new BLE2902());
        pBase->start();
        pAdvertising = BLEDevice::getAdvertising();
        pAdvertising->addServiceUUID(BLEUUID(svc));
        pAdvertising->addServiceUUID(BLEUUID("6E400001-B5A3-F393-E0A9-E50E24DCCA9E"));
        pAdvertising->setScanResponse(true);
        pAdvertising->start();
        btActive = true;
    }
}

void disableBluetooth()
{
    if (pAdvertising)
    {
        pAdvertising->stop();
        pAdvertising = nullptr;
        pTXChar = nullptr;
        pRXChar = nullptr;
        pTXCharBase = nullptr;
        pRXCharBase = nullptr;
        btActive = false;
    }
}

void setDefaultsHeltec()
{
    cfg.deviceType = 0;
    cfg.csPin = 8;
    cfg.resetPin = 12;
    cfg.busyPin = 13;
    cfg.dioPin = 14;
    cfg.frequency = 868.0;
    cfg.bandwidth = 125.0;
    cfg.spreadingFactor = 7;
    cfg.codingRate = 5;
    cfg.syncWord = 0x12;
    cfg.preambleLength = 8;
    cfg.tcxoVoltage = 1.8;
    cfg.useDio2AsRfSwitch = false;
    cfg.btEnabled = true;
    cfg.pwr = 14;
    cfg.sclkPin = 9;
    cfg.misoPin = 11;
    cfg.mosiPin = 10;
    cfg.nssPin = 8;
    cfg.rstPin = 12;
    cfg.dio0Pin = 13;
    cfg.dio1Pin = 14;
}

void setDefaultsWroom()
{
    cfg.deviceType = 1;
    cfg.csPin = 5;
    cfg.resetPin = 14;
    cfg.busyPin = 26;
    cfg.dioPin = 35;
    cfg.frequency = 868.0;
    cfg.bandwidth = 125.0;
    cfg.spreadingFactor = 9;
    cfg.codingRate = 7;
    cfg.syncWord = 0x12;
    cfg.preambleLength = 22;
    cfg.tcxoVoltage = 1.6;
    cfg.useDio2AsRfSwitch = false;
    cfg.btEnabled = true;
    cfg.pwr = 17;
    cfg.sclkPin = 18;
    cfg.misoPin = 19;
    cfg.mosiPin = 23;
    cfg.nssPin = 5;
    cfg.rstPin = 14;
    cfg.dio0Pin = 26;
    cfg.dio1Pin = 35;
}

void enforceHeltecV3Pins()
{
    cfg.csPin = 8;
    cfg.resetPin = 12;
    cfg.busyPin = 13;
    cfg.dioPin = 14;
    cfg.sclkPin = 9;
    cfg.misoPin = 11;
    cfg.mosiPin = 10;
    cfg.nssPin = 8;
    cfg.rstPin = 12;
    cfg.dio0Pin = 13;
    cfg.dio1Pin = 14;
    if (cfg.tcxoVoltage <= 0.01f)
        cfg.tcxoVoltage = 1.8;
}

const char *getDeviceProfileKey()
{
    return cfg.deviceType == 0 ? "heltec_v3" : "megamesh";
}

static bool putBytesIfChanged(const char *ns, const char *key, const void *data, size_t len)
{
    bool same = false;

    prefs.begin(ns, true);
    size_t existingLen = prefs.getBytesLength(key);
    if (existingLen == len)
    {
        uint8_t *existing = (uint8_t *)malloc(len);
        if (existing)
        {
            prefs.getBytes(key, existing, len);
            same = memcmp(existing, data, len) == 0;
            free(existing);
        }
    }
    prefs.end();

    if (same)
        return false;

    prefs.begin(ns, false);
    prefs.putBytes(key, data, len);
    prefs.end();
    return true;
}

void saveConfig()
{
    cfg.magic = CFG_MAGIC;
    putBytesIfChanged("lora", "cfg", &cfg, sizeof(cfg));
    saveWeatherConfig();
    configSaved = true;
    sendMsg("{\"evt\":\"cfg_saved\"}");
}

void saveWeatherConfig()
{
    WeatherPersistConfig wx;
    wx.magic = WX_MAGIC;
    wx.weatherMode = weatherModeEnabled ? 1 : 0;
    wx.weatherIntervalMs = weatherIntervalMs;
    wx.sensorCount = sensorCount;
    for (uint8_t i = 0; i < MAX_SENSORS; i++)
    {
        if (i < sensorCount)
            wx.sensors[i] = sensors[i];
        else
        {
            wx.sensors[i].pin = 0;
            wx.sensors[i].analog = false;
        }
    }

    putBytesIfChanged("lora", "wxcfg", &wx, sizeof(wx));
}

void loadWeatherConfig()
{
    sensorCount = 0;

    prefs.begin("lora", true);
    size_t len = prefs.getBytesLength("wxcfg");
    if (len == sizeof(WeatherPersistConfig))
    {
        WeatherPersistConfig wx;
        prefs.getBytes("wxcfg", &wx, sizeof(wx));
        prefs.end();

        if (wx.magic == WX_MAGIC)
        {
            weatherModeEnabled = wx.weatherMode != 0;
            weatherIntervalMs = wx.weatherIntervalMs < 500 ? 500 : wx.weatherIntervalMs;
            sensorCount = wx.sensorCount > MAX_SENSORS ? MAX_SENSORS : wx.sensorCount;

            for (uint8_t i = 0; i < sensorCount; i++)
            {
                sensors[i] = wx.sensors[i];
                pinMode(sensors[i].pin, sensors[i].analog ? INPUT : INPUT_PULLUP);
            }
        }
        return;
    }
    prefs.end();
}

bool loadConfig()
{
    struct LegacyLoraConfig
    {
        uint32_t magic;
        uint8_t deviceType;
        uint8_t csPin;
        uint8_t resetPin;
        uint8_t busyPin;
        uint8_t dioPin;
        float frequency;
        float bandwidth;
        uint8_t spreadingFactor;
        uint8_t codingRate;
        uint8_t syncWord;
        uint16_t preambleLength;
        float tcxoVoltage;
        bool useDio2AsRfSwitch;
        bool btEnabled;
    };

    prefs.begin("lora", true);
    size_t len = prefs.getBytesLength("cfg");
    if (len == sizeof(cfg))
    {
        prefs.getBytes("cfg", &cfg, sizeof(cfg));
        prefs.end();
        if (cfg.magic == CFG_MAGIC)
        {
            if (cfg.deviceType == 0)
            {
                enforceHeltecV3Pins();
            }
            loadWeatherConfig();
            configSaved = true;
            return true;
        }
    }

    if (len == sizeof(LegacyLoraConfig))
    {
        LegacyLoraConfig oldCfg;
        prefs.getBytes("cfg", &oldCfg, sizeof(oldCfg));
        prefs.end();
        if (oldCfg.magic == CFG_MAGIC)
        {
            cfg.magic = oldCfg.magic;
            cfg.deviceType = oldCfg.deviceType;
            cfg.csPin = oldCfg.csPin;
            cfg.resetPin = oldCfg.resetPin;
            cfg.busyPin = oldCfg.busyPin;
            cfg.dioPin = oldCfg.dioPin;
            cfg.frequency = oldCfg.frequency;
            cfg.bandwidth = oldCfg.bandwidth;
            cfg.spreadingFactor = oldCfg.spreadingFactor;
            cfg.codingRate = oldCfg.codingRate;
            cfg.syncWord = oldCfg.syncWord;
            cfg.preambleLength = oldCfg.preambleLength;
            cfg.tcxoVoltage = oldCfg.tcxoVoltage;
            cfg.useDio2AsRfSwitch = oldCfg.useDio2AsRfSwitch;
            cfg.btEnabled = oldCfg.btEnabled;

            if (cfg.deviceType == 0)
            {
                // migrate legacy Heltec profiles to V3 pin mapping
                cfg.pwr = 14;
                enforceHeltecV3Pins();
            }
            else
            {
                cfg.pwr = 17;
                cfg.sclkPin = 18;
                cfg.misoPin = 19;
                cfg.mosiPin = 23;
                cfg.nssPin = cfg.csPin;
                cfg.rstPin = cfg.resetPin;
                cfg.dio0Pin = cfg.busyPin;
                cfg.dio1Pin = cfg.dioPin;
            }

            loadWeatherConfig();
            configSaved = true;
            return true;
        }
    }

    prefs.end();
    return false;
}

String readLine(Stream &s)
{
    String line = s.readStringUntil('\n');
    line.trim();
    return line;
}

// send a message to USB serial and to BT if active
void sendMsg(const String &s)
{
    Serial.println(s);
    // also notify via BLE TX characteristic when connected
    if (btActive && (pTXChar || pTXCharBase))
    {
        const char *raw = s.c_str();
        size_t len = s.length();
        const size_t maxChunk = 180;

        for (size_t off = 0; off < len; off += maxChunk)
        {
            size_t chunkLen = len - off;
            if (chunkLen > maxChunk)
                chunkLen = maxChunk;
            bool hasMore = (off + chunkLen) < len;

            if (pTXChar)
            {
                pTXChar->setValue((uint8_t *)(raw + off), chunkLen);
                pTXChar->notify();
            }
            if (pTXCharBase)
            {
                pTXCharBase->setValue((uint8_t *)(raw + off), chunkLen);
                pTXCharBase->notify();
            }

            if (hasMore)
                delay(5);
        }
    }
}

void sendConfig()
{
    String j = "{";
    j.reserve(420);
    j += "\"device\":\"" + String(getDeviceProfileKey()) + "\"";
    j += ",\"cs\":" + String(cfg.csPin);
    j += ",\"reset\":" + String(cfg.resetPin);
    j += ",\"busy\":" + String(cfg.busyPin);
    j += ",\"dio\":" + String(cfg.dioPin);
    j += ",\"freq\":" + String(cfg.frequency);
    j += ",\"bw\":" + String(cfg.bandwidth);
    j += ",\"sf\":" + String(cfg.spreadingFactor);
    j += ",\"cr\":" + String(cfg.codingRate);
    j += ",\"sync\":\"0x" + String(cfg.syncWord, HEX) + "\"";
    j += ",\"pwr\":" + String(cfg.pwr);
    j += ",\"sclk\":" + String(cfg.sclkPin);
    j += ",\"miso\":" + String(cfg.misoPin);
    j += ",\"mosi\":" + String(cfg.mosiPin);
    j += ",\"nss\":" + String(cfg.nssPin);
    j += ",\"rst\":" + String(cfg.rstPin);
    j += ",\"dio0\":" + String(cfg.dio0Pin);
    j += ",\"dio1\":" + String(cfg.dio1Pin);
    j += ",\"preamble\":" + String(cfg.preambleLength);
    j += ",\"tcxo\":" + String(cfg.tcxoVoltage);
    j += ",\"dio2\":" + String(cfg.useDio2AsRfSwitch ? 1 : 0);
    j += ",\"bt\":" + String(cfg.btEnabled ? 1 : 0);
    j += ",\"mesh_enc\":" + String(meshEncryptionEnabled ? 1 : 0);
    j += ",\"weather\":" + String(weatherModeEnabled ? 1 : 0);
    j += ",\"weather_interval\":" + String(weatherIntervalMs);
    j += ",\"weather_sensors\":" + String(sensorCount);
    j += "}";
    sendMsg(j);
}

static String toHexByte(uint8_t b)
{
    const char *hex = "0123456789ABCDEF";
    String s;
    s.reserve(2);
    s += hex[(b >> 4) & 0x0F];
    s += hex[b & 0x0F];
    return s;
}

static String jsonEscape(const String &in)
{
    String out;
    out.reserve(in.length() * 2 + 8);
    for (size_t i = 0; i < in.length(); ++i)
    {
        uint8_t c = (uint8_t)in[i];
        if (c == '"')
            out += "\\\"";
        else if (c == '\\')
            out += "\\\\";
        else if (c == '\n')
            out += "\\n";
        else if (c == '\r')
            out += "\\r";
        else if (c == '\t')
            out += "\\t";
        else if (c >= 32 && c <= 126)
            out += (char)c;
        else
        {
            out += "\\u00";
            out += toHexByte(c);
        }
    }
    return out;
}

static bool parseHexKey16(const String &hexIn, uint8_t out[16])
{
    String s = hexIn;
    s.trim();
    if (s.startsWith("0x") || s.startsWith("0X"))
    {
        s = s.substring(2);
    }
    if (s.length() != 32)
        return false;

    for (int i = 0; i < 16; i++)
    {
        char hi = s.charAt(i * 2);
        char lo = s.charAt(i * 2 + 1);
        int h = -1;
        int l = -1;
        if (hi >= '0' && hi <= '9')
            h = hi - '0';
        else if (hi >= 'a' && hi <= 'f')
            h = 10 + (hi - 'a');
        else if (hi >= 'A' && hi <= 'F')
            h = 10 + (hi - 'A');

        if (lo >= '0' && lo <= '9')
            l = lo - '0';
        else if (lo >= 'a' && lo <= 'f')
            l = 10 + (lo - 'a');
        else if (lo >= 'A' && lo <= 'F')
            l = 10 + (lo - 'A');

        if (h < 0 || l < 0)
            return false;
        out[i] = (uint8_t)((h << 4) | l);
    }
    return true;
}

static String meshKeyHex()
{
    String s;
    s.reserve(32);
    for (uint8_t i = 0; i < 16; i++)
    {
        s += toHexByte(meshKey[i]);
    }
    return s;
}

static void updatePeer(uint16_t id)
{
    if (id == 0)
        return;

    for (uint8_t i = 0; i < peerCount; i++)
    {
        if (peers[i].id == id)
        {
            peers[i].lastSeenMs = millis();
            return;
        }
    }

    if (peerCount < MAX_PEERS)
    {
        peers[peerCount].id = id;
        peers[peerCount].lastSeenMs = millis();
        peerCount++;
    }
}

static bool encryptCtr(const uint8_t *plain, uint8_t *cipher, size_t len, const uint8_t nonce[8], uint32_t counter)
{
    if (!meshEncryptionEnabled)
    {
        memcpy(cipher, plain, len);
        return true;
    }

    mbedtls_aes_context ctx;
    mbedtls_aes_init(&ctx);
    if (mbedtls_aes_setkey_enc(&ctx, meshKey, 128) != 0)
    {
        mbedtls_aes_free(&ctx);
        return false;
    }

    uint8_t nonceCounter[16] = {0};
    memcpy(nonceCounter, nonce, 8);
    nonceCounter[8] = (uint8_t)((counter >> 24) & 0xFF);
    nonceCounter[9] = (uint8_t)((counter >> 16) & 0xFF);
    nonceCounter[10] = (uint8_t)((counter >> 8) & 0xFF);
    nonceCounter[11] = (uint8_t)(counter & 0xFF);

    uint8_t streamBlock[16] = {0};
    size_t ncOff = 0;
    int rc = mbedtls_aes_crypt_ctr(&ctx, len, &ncOff, nonceCounter, streamBlock, plain, cipher);
    mbedtls_aes_free(&ctx);
    return rc == 0;
}

static bool decryptCtr(const uint8_t *cipher, uint8_t *plain, size_t len, const uint8_t nonce[8], uint32_t counter)
{
    return encryptCtr(cipher, plain, len, nonce, counter);
}

static bool sendMeshPacket(uint8_t type, uint16_t dst, const uint8_t *payload, uint8_t payloadLen)
{
    if (!radioInitialized || !radioPtr)
        return false;

    if (payloadLen > 120)
        return false;

    uint8_t frame[160] = {0};
    uint8_t enc[120] = {0};
    uint8_t nonce[8] = {0};
    uint32_t n0 = esp_random();
    uint32_t n1 = esp_random();
    memcpy(nonce, &n0, 4);
    memcpy(nonce + 4, &n1, 4);

    bool plainMgmt = (type == 0x01 || type == 0x02 || type == 0x30 || type == 0x31);
    if (plainMgmt)
    {
        memcpy(enc, payload, payloadLen);
    }
    else
    {
        if (!encryptCtr(payload, enc, payloadLen, nonce, meshTxCounter))
            return false;
    }

    size_t idx = 0;
    frame[idx++] = 0x4D;
    frame[idx++] = 0x58;
    frame[idx++] = 0x01; // protocol version
    frame[idx++] = type;
    frame[idx++] = (uint8_t)((nodeId >> 8) & 0xFF);
    frame[idx++] = (uint8_t)(nodeId & 0xFF);
    frame[idx++] = (uint8_t)((dst >> 8) & 0xFF);
    frame[idx++] = (uint8_t)(dst & 0xFF);
    frame[idx++] = (uint8_t)((meshTxCounter >> 24) & 0xFF);
    frame[idx++] = (uint8_t)((meshTxCounter >> 16) & 0xFF);
    frame[idx++] = (uint8_t)((meshTxCounter >> 8) & 0xFF);
    frame[idx++] = (uint8_t)(meshTxCounter & 0xFF);
    memcpy(frame + idx, nonce, 8);
    idx += 8;
    frame[idx++] = payloadLen;
    memcpy(frame + idx, enc, payloadLen);
    idx += payloadLen;

    uint16_t crc = crc16_ccitt(frame, idx);
    frame[idx++] = (uint8_t)((crc >> 8) & 0xFF);
    frame[idx++] = (uint8_t)(crc & 0xFF);

    meshTxCounter++;
    int16_t txState = radioPtr->transmit(frame, idx);
    return txState == RADIOLIB_ERR_NONE;
}

static void sendDiscoveryResponse(uint16_t dst)
{
    String p = String("NODE:") + String(nodeId);
    sendMeshPacket(0x02, dst, (const uint8_t *)p.c_str(), (uint8_t)p.length());
}

void broadcastDiscovery()
{
    String p = String("DISCOVER:") + String(nodeId);
    if (sendMeshPacket(0x01, 0xFFFF, (const uint8_t *)p.c_str(), (uint8_t)p.length()))
        sendMsg("{\"evt\":\"scan_started\"}");
    else
        sendMsg("{\"evt\":\"radio_err\",\"code\":-999}");
}

void sendPeerList()
{
    String j = "{\"evt\":\"peers\",\"items\":[";
    j.reserve(220);
    for (uint8_t i = 0; i < peerCount; i++)
    {
        if (i)
            j += ",";
        j += "{\"id\":" + String(peers[i].id) + ",\"ageMs\":" + String(millis() - peers[i].lastSeenMs) + "}";
    }
    j += "]}";
    sendMsg(j);
}

static bool addSensor(uint8_t pin, bool analogMode)
{
    for (uint8_t i = 0; i < sensorCount; i++)
    {
        if (sensors[i].pin == pin)
        {
            sensors[i].analog = analogMode;
            return true;
        }
    }

    if (sensorCount >= MAX_SENSORS)
        return false;

    sensors[sensorCount].pin = pin;
    sensors[sensorCount].analog = analogMode;
    pinMode(pin, analogMode ? INPUT : INPUT_PULLUP);
    sensorCount++;
    return true;
}

void sendWeatherPacket()
{
    if (!meshRunning || !radioInitialized)
        return;

    String p = String("WX:") + String(nodeId);
    for (uint8_t i = 0; i < sensorCount; i++)
    {
        int value = sensors[i].analog ? analogRead(sensors[i].pin) : digitalRead(sensors[i].pin);
        p += ";";
        p += String(sensors[i].pin);
        p += ":";
        p += String(value);
    }

    if (sendMeshPacket(0x10, 0xFFFF, (const uint8_t *)p.c_str(), (uint8_t)p.length()))
    {
        sendMsg(String("{\"evt\":\"weather_tx\",\"sensors\":") + sensorCount + "}");
    }
}

static bool sendWebsiteMessage(uint16_t dst, const String &payload)
{
    if (!meshRunning || !radioInitialized)
        return false;

    String cleanPayload = payload;
    cleanPayload.trim();
    if (!cleanPayload.length() || cleanPayload.length() > 120)
        return false;

    return sendMeshPacket(0x20, dst, (const uint8_t *)cleanPayload.c_str(), (uint8_t)cleanPayload.length());
}

static bool initRadioRobust(bool emitEvents)
{
    int16_t lastState = -999;

    auto beginWith = [&](float tcxo, uint8_t pwr) -> int16_t
    {
        if (radioPtr)
        {
            delete radioPtr;
            radioPtr = nullptr;
        }

        Module *mod = new Module(cfg.nssPin, cfg.dio1Pin, cfg.rstPin, cfg.dio0Pin);
        radioPtr = new SX1262(mod);
        return radioPtr->begin(cfg.frequency, cfg.bandwidth, cfg.spreadingFactor, cfg.codingRate, cfg.syncWord, pwr, cfg.preambleLength, tcxo, false);
    };

    auto tryProfile = [&](bool enforcePins) -> bool
    {
        if (enforcePins && cfg.deviceType == 0)
        {
            enforceHeltecV3Pins();
        }

        const float tcxoTry[] = {cfg.tcxoVoltage, 1.8f, 1.6f, 0.0f};
        const uint8_t pwrTry[] = {cfg.pwr, (uint8_t)14, (uint8_t)10};

        for (uint8_t p = 0; p < 3; p++)
        {
            uint8_t outPwr = pwrTry[p];
            bool pwrSeen = false;
            for (uint8_t pp = 0; pp < p; pp++)
            {
                if (pwrTry[pp] == outPwr)
                {
                    pwrSeen = true;
                    break;
                }
            }
            if (pwrSeen)
                continue;

            for (uint8_t i = 0; i < 4; i++)
            {
                float tcxo = tcxoTry[i];
                bool tcxoSeen = false;
                for (uint8_t k = 0; k < i; k++)
                {
                    float diff = tcxoTry[k] - tcxo;
                    if (diff < 0)
                        diff = -diff;
                    if (diff < 0.01f)
                    {
                        tcxoSeen = true;
                        break;
                    }
                }
                if (tcxoSeen)
                    continue;

                int16_t state = beginWith(tcxo, outPwr);
                lastState = state;
                if (state == RADIOLIB_ERR_NONE)
                {
                    cfg.tcxoVoltage = tcxo;
                    cfg.pwr = outPwr;
                    if (emitEvents)
                    {
                        sendMsg(String("{\"evt\":\"tcxo_auto\",\"v\":") + String(cfg.tcxoVoltage, 1) + "}");
                    }
                    return true;
                }
            }
        }

        return false;
    };

    bool ok = tryProfile(false);
    if (!ok && cfg.deviceType == 0)
    {
        ok = tryProfile(true);
    }

    radioInitialized = ok;
    if (ok)
    {
        if (emitEvents)
            sendMsg("{\"evt\":\"radio_ready\"}");
        return true;
    }

    if (emitEvents)
        sendMsg(String("{\"evt\":\"radio_err\",\"code\":") + lastState + "}");
    return false;
}

static bool ensureMeshRunning(bool emitEvents)
{
    if (!radioInitialized)
    {
        if (!initRadioRobust(emitEvents))
        {
            if (emitEvents)
                sendMsg("{\"evt\":\"radio_not_ready\"}");
            return false;
        }
    }

    uint64_t mac = ESP.getEfuseMac();
    nodeId = (uint16_t)(mac & 0xFFFF);
    meshRunning = true;
    if (emitEvents)
        sendMsg(String("{\"evt\":\"mesh_started\",\"nodeId\":") + nodeId + "}");
    return true;
}

static void handleMeshFrame(uint8_t *buf, size_t len)
{
    if (len < 22)
        return;
    if (buf[0] != 0x4D || buf[1] != 0x58)
        return;

    uint16_t frameCrc = ((uint16_t)buf[len - 2] << 8) | buf[len - 1];
    uint16_t calc = crc16_ccitt(buf, len - 2);
    if (frameCrc != calc)
        return;

    uint8_t type = buf[3];
    uint16_t src = ((uint16_t)buf[4] << 8) | buf[5];
    uint16_t dst = ((uint16_t)buf[6] << 8) | buf[7];
    uint32_t ctr = ((uint32_t)buf[8] << 24) | ((uint32_t)buf[9] << 16) | ((uint32_t)buf[10] << 8) | (uint32_t)buf[11];
    uint8_t nonce[8];
    memcpy(nonce, buf + 12, 8);
    uint8_t payloadLen = buf[20];
    if (21 + payloadLen + 2 != len)
        return;
    if (!(dst == 0xFFFF || dst == nodeId))
        return;

    uint8_t plain[120] = {0};
    bool plainMgmt = (type == 0x01 || type == 0x02 || type == 0x30 || type == 0x31);
    if (plainMgmt)
    {
        memcpy(plain, buf + 21, payloadLen);
    }
    else
    {
        if (!decryptCtr(buf + 21, plain, payloadLen, nonce, ctr))
            return;
    }

    updatePeer(src);
    String payload;
    payload.reserve(payloadLen);
    for (uint8_t i = 0; i < payloadLen; i++)
        payload += (char)plain[i];
    String payloadJson = jsonEscape(payload);

    if (type == 0x01)
    {
        sendDiscoveryResponse(src);
        sendMsg(String("{\"evt\":\"peer_found\",\"id\":") + src + "}");
        return;
    }
    if (type == 0x02)
    {
        sendMsg(String("{\"evt\":\"peer_found\",\"id\":") + src + "}");
        return;
    }
    if (type == 0x10)
    {
        String j = "{\"evt\":\"weather_rx\",\"from\":" + String(src) + ",\"data\":\"";
        j += payloadJson;
        j += "\"}";
        sendMsg(j);
        return;
    }
    if (type == 0x20)
    {
        String j = "{\"evt\":\"msg_rx\",\"from\":" + String(src) + ",\"data\":\"";
        j += payloadJson;
        j += "\"}";
        sendMsg(j);
        return;
    }
    if (type == 0x30)
    {
        if (payload.startsWith("KEY:"))
        {
            String keyHex = payload.substring(4);
            uint8_t parsed[16];
            if (parseHexKey16(keyHex, parsed))
            {
                memcpy(meshKey, parsed, 16);
                sendMsg(String("{\"evt\":\"mesh_key_rx\",\"from\":") + src + ",\"v\":\"" + meshKeyHex() + "\"}");
                String ack = "KEY_OK";
                sendMeshPacket(0x31, src, (const uint8_t *)ack.c_str(), (uint8_t)ack.length());
                return;
            }
        }
        sendMsg(String("{\"evt\":\"mesh_key_rx_err\",\"from\":") + src + "}");
        return;
    }
    if (type == 0x31)
    {
        sendMsg(String("{\"evt\":\"mesh_key_ack\",\"from\":") + src + "}");
        return;
    }

    String j = "{\"evt\":\"mesh_rx\",\"from\":" + String(src) + ",\"t\":" + String(type) + ",\"data\":\"";
    j += payloadJson;
    j += "\"}";
    sendMsg(j);
}

void handleCommand(String raw, bool fromBT = false)
{
    (void)fromBT;
    String rawCmd = raw;
    rawCmd.trim();
    String cmd = rawCmd;
    cmd.toLowerCase();
    if (cmd == "bt on")
    {
        cfg.btEnabled = true;
        // enable immediately and make discoverable
        enableBluetoothVisible("ESP32-LoRaCfg");
        sendMsg("{\"evt\":\"bt_on\"}");
        return;
    }
    if (cmd == "bt off")
    {
        cfg.btEnabled = false;
        disableBluetooth();
        sendMsg("{\"evt\":\"bt_off\"}");
        return;
    }
    if (cmd == "help")
    {
        sendMsg("{\"cmds\":\"setup|info|set|device <heltec_v3|megamesh>|save|init|autostart|startmesh|reboot|bt off|mesh scan|mesh peers|mesh key|mesh key <hex>|mesh keysend <nodeId>|mesh keygen|mesh enc <on|off>|weather on|weather off|weather interval <ms>|weather add <pin> <a|d>|weather clear|weather now|send <msg>|sendto <nodeId> <msg>\"}");
        return;
    }
    if (cmd == "show")
    {
        sendConfig();
        return;
    }
    if (cmd == "info")
    {
        sendSetupInfo();
        return;
    }
    if (cmd.startsWith("device "))
    {
        String deviceType = cmd.substring(7);
        deviceType.trim();
        if (deviceType == "heltec" || deviceType == "heltec_v3")
            setDefaultsHeltec();
        else if (deviceType == "wroom" || deviceType == "megamesh")
            setDefaultsWroom();
        else
        {
            sendMsg("{\"evt\":\"unknown_cmd\"}");
            return;
        }
        sendMsg("{\"evt\":\"defaults_applied\"}");
        sendSetupInfo();
        return;
    }
    if (cmd.startsWith("set "))
    {
        int sp = cmd.indexOf(' ', 4);
        if (sp < 0)
        {
            sendMsg("{\"evt\":\"unknown_cmd\"}");
            return;
        }
        String key = cmd.substring(4, sp);
        String val = cmd.substring(sp + 1);
        key.trim();
        val.trim();
        if (key == "cs")
        {
            cfg.csPin = val.toInt();
            cfg.nssPin = cfg.csPin;
        }
        else if (key == "nss")
        {
            cfg.nssPin = val.toInt();
            cfg.csPin = cfg.nssPin;
        }
        else if (key == "reset")
        {
            cfg.resetPin = val.toInt();
            cfg.rstPin = cfg.resetPin;
        }
        else if (key == "rst")
        {
            cfg.rstPin = val.toInt();
            cfg.resetPin = cfg.rstPin;
        }
        else if (key == "busy")
        {
            cfg.busyPin = val.toInt();
            cfg.dio0Pin = cfg.busyPin;
        }
        else if (key == "dio")
        {
            cfg.dioPin = val.toInt();
            cfg.dio1Pin = cfg.dioPin;
        }
        else if (key == "dio0")
        {
            cfg.dio0Pin = val.toInt();
            cfg.busyPin = cfg.dio0Pin;
        }
        else if (key == "dio1")
        {
            cfg.dio1Pin = val.toInt();
            cfg.dioPin = cfg.dio1Pin;
        }
        else if (key == "sclk")
            cfg.sclkPin = val.toInt();
        else if (key == "miso")
            cfg.misoPin = val.toInt();
        else if (key == "mosi")
            cfg.mosiPin = val.toInt();
        else if (key == "freq")
            cfg.frequency = val.toFloat();
        else if (key == "bw")
            cfg.bandwidth = val.toFloat();
        else if (key == "sf")
            cfg.spreadingFactor = val.toInt();
        else if (key == "cr")
            cfg.codingRate = val.toInt();
        else if (key == "pwr")
            cfg.pwr = val.toInt();
        else if (key == "sync" || key == "sw")
        {
            if (val.startsWith("0x"))
                cfg.syncWord = (uint8_t)strtoul(val.c_str() + 2, NULL, 16);
            else
                cfg.syncWord = val.toInt();
        }
        else if (key == "preamble")
            cfg.preambleLength = val.toInt();
        else if (key == "tcxo")
            cfg.tcxoVoltage = val.toFloat();
        else if (key == "dio2")
            cfg.useDio2AsRfSwitch = (val == "1" || val == "true");
        else if (key == "weather" || key == "wx")
            weatherModeEnabled = (val == "1" || val == "true" || val == "on");
        else if (key == "weather_interval" || key == "wxint")
        {
            uint32_t v = (uint32_t)val.toInt();
            weatherIntervalMs = v < 500 ? 500 : v;
        }
        else
        {
            sendMsg("{\"evt\":\"unknown_cmd\"}");
            return;
        }
        sendMsg("{\"evt\":\"ok\"}");
        return;
    }
    if (cmd == "setup")
    {
        if (setupModeActive)
        {
            sendMsg("{\"evt\":\"setup_already\"}");
            return;
        }
        startConfigMode();
        return;
    }
    if (cmd == "save")
    {
        if (!setupModeActive)
        {
            saveConfig();
            sendMsg("{\"evt\":\"cfg_staged\"}");
            return;
        }
        setupSaveRequested = true;
        sendMsg("{\"evt\":\"cfg_staged\"}");
        return;
    }
    if (cmd == "init")
    {
        initRadioRobust(true);
        return;
    }
    if (cmd == "autostart")
    {
        if (setupModeActive)
            setupSaveRequested = true;
        saveConfig();
        if (ensureMeshRunning(true))
            sendMsg("{\"evt\":\"autostart_ok\"}");
        else
            sendMsg("{\"evt\":\"autostart_err\"}");
        return;
    }
    if (cmd == "startmesh")
    {
        ensureMeshRunning(true);
        return;
    }
    if (cmd == "mesh scan")
    {
        if (!ensureMeshRunning(true))
        {
            sendMsg("{\"evt\":\"radio_not_ready\"}");
            return;
        }
        broadcastDiscovery();
        return;
    }
    if (cmd == "mesh peers")
    {
        sendPeerList();
        return;
    }
    if (cmd == "mesh key")
    {
        sendMsg(String("{\"evt\":\"mesh_key\",\"v\":\"") + meshKeyHex() + "\"}");
        return;
    }
    if (cmd.startsWith("mesh keysend "))
    {
        if (!meshRunning || !radioInitialized)
        {
            sendMsg("{\"evt\":\"radio_not_ready\"}");
            return;
        }
        uint16_t dst = (uint16_t)cmd.substring(13).toInt();
        String payload = String("KEY:") + meshKeyHex();
        if (sendMeshPacket(0x30, dst, (const uint8_t *)payload.c_str(), (uint8_t)payload.length()))
        {
            sendMsg(String("{\"evt\":\"mesh_key_tx\",\"dst\":") + dst + "}");
        }
        else
        {
            sendMsg(String("{\"evt\":\"mesh_key_tx_err\",\"dst\":") + dst + "}");
        }
        return;
    }
    if (cmd == "mesh keygen")
    {
        for (uint8_t i = 0; i < 16; i += 4)
        {
            uint32_t r = esp_random();
            meshKey[i + 0] = (uint8_t)(r & 0xFF);
            meshKey[i + 1] = (uint8_t)((r >> 8) & 0xFF);
            meshKey[i + 2] = (uint8_t)((r >> 16) & 0xFF);
            meshKey[i + 3] = (uint8_t)((r >> 24) & 0xFF);
        }
        sendMsg(String("{\"evt\":\"mesh_key\",\"v\":\"") + meshKeyHex() + "\"}");
        return;
    }
    if (cmd.startsWith("mesh key "))
    {
        String keyHex = cmd.substring(9);
        uint8_t parsed[16];
        if (!parseHexKey16(keyHex, parsed))
        {
            sendMsg("{\"evt\":\"radio_err\",\"code\":-910}");
            return;
        }
        memcpy(meshKey, parsed, 16);
        sendMsg(String("{\"evt\":\"mesh_key\",\"v\":\"") + meshKeyHex() + "\"}");
        sendMsg("{\"evt\":\"ok\"}");
        return;
    }
    if (cmd.startsWith("mesh enc "))
    {
        String mode = cmd.substring(9);
        mode.trim();
        meshEncryptionEnabled = (mode == "on" || mode == "1" || mode == "true");
        sendMsg(String("{\"evt\":\"mesh_enc\",\"v\":") + (meshEncryptionEnabled ? "1}" : "0}"));
        return;
    }
    if (cmd == "weather on")
    {
        weatherModeEnabled = true;
        sendMsg("{\"evt\":\"ok\"}");
        return;
    }
    if (cmd == "weather off")
    {
        weatherModeEnabled = false;
        sendMsg("{\"evt\":\"ok\"}");
        return;
    }
    if (cmd.startsWith("weather interval "))
    {
        uint32_t v = (uint32_t)cmd.substring(17).toInt();
        if (v < 500)
            v = 500;
        weatherIntervalMs = v;
        sendMsg("{\"evt\":\"ok\"}");
        return;
    }
    if (cmd.startsWith("weather add "))
    {
        int sp = cmd.indexOf(' ', 12);
        if (sp < 0)
        {
            sendMsg("{\"evt\":\"unknown_cmd\"}");
            return;
        }
        uint8_t pin = (uint8_t)cmd.substring(12, sp).toInt();
        String mode = cmd.substring(sp + 1);
        mode.trim();
        bool analogMode = (mode == "a" || mode == "analog");
        if (!addSensor(pin, analogMode))
        {
            sendMsg("{\"evt\":\"radio_err\",\"code\":-911}");
            return;
        }
        sendMsg("{\"evt\":\"ok\"}");
        return;
    }
    if (cmd == "weather clear")
    {
        sensorCount = 0;
        sendMsg("{\"evt\":\"ok\"}");
        return;
    }
    if (cmd == "weather now")
    {
        sendWeatherPacket();
        return;
    }
    if (cmd.startsWith("sendto "))
    {
        int idSep = cmd.indexOf(' ', 7);
        if (idSep < 0)
        {
            sendMsg("{\"evt\":\"unknown_cmd\"}");
            return;
        }

        uint16_t dst = (uint16_t)rawCmd.substring(7, idSep).toInt();
        String payload = rawCmd.substring(idSep + 1);
        if (sendWebsiteMessage(dst, payload))
            sendMsg(String("{\"evt\":\"msg_tx\",\"dst\":") + dst + ",\"len\":" + payload.length() + "}");
        else
            sendMsg("{\"evt\":\"msg_tx_err\"}");
        return;
    }
    if (cmd.startsWith("send "))
    {
        String payload = rawCmd.substring(5);
        if (sendWebsiteMessage(0xFFFF, payload))
            sendMsg(String("{\"evt\":\"msg_tx\",\"dst\":65535,\"len\":") + payload.length() + "}");
        else
            sendMsg("{\"evt\":\"msg_tx_err\"}");
        return;
    }
    if (cmd == "reboot")
    {
        sendMsg("{\"evt\":\"rebooting\"}");
        delay(200);
        ESP.restart();
        return;
    }
    sendMsg("{\"evt\":\"unknown_cmd\"}");
}

void checkInputs()
{
    if (Serial.available())
    {
        String l = readLine(Serial);
        if (l.length())
            handleCommand(l, false);
    }
}

// Wait until at least one input arrives (Serial or BLE) and process it.
// Returns true when the given flag pointer becomes true.
static bool waitForFlag(bool *flag)
{
    while (!(*flag))
    {
        checkInputs();
        delay(20);
    }
    return true;
}

// Send setup_info: current defaults + list of all configurable fields with
// their current values and accepted value ranges so the client knows what
// to ask for.
void sendSetupInfo()
{
    String syncHex = String(cfg.syncWord, HEX);
    syncHex.toUpperCase();
    if (syncHex.length() < 2)
        syncHex = "0" + syncHex;

    String j = "{\"evt\":\"setup_info\"";
    j.reserve(1200);
    j += ",\"device\":\"" + String(getDeviceProfileKey()) + "\"";
    j += ",\"first_setup\":" + String(configSaved ? "false" : "true");
    j += ",\"fields\":[";
    j += "{\"k\":\"device\",\"v\":\"" + String(getDeviceProfileKey()) + "\",\"opts\":\"heltec_v3|megamesh\"}";
    j += ",{\"k\":\"freq\",\"v\":" + String(cfg.frequency, 1) + ",\"unit\":\"MHz\",\"opts\":\"433.0|868.0|869.5|915.0\",\"min\":137.0,\"max\":1020.0}";
    j += ",{\"k\":\"bw\",\"v\":" + String(cfg.bandwidth, 1) + ",\"unit\":\"kHz\",\"opts\":\"7.8|10.4|15.6|20.8|31.25|41.7|62.5|125|250|500\"}";
    j += ",{\"k\":\"sf\",\"v\":" + String(cfg.spreadingFactor) + ",\"opts\":\"6|7|8|9|10|11|12\",\"min\":6,\"max\":12}";
    j += ",{\"k\":\"cr\",\"v\":" + String(cfg.codingRate) + ",\"opts\":\"5|6|7|8\"}";
    j += ",{\"k\":\"pwr\",\"v\":" + String(cfg.pwr) + ",\"unit\":\"dBm\",\"opts\":\"2|10|14|17|20|22\",\"min\":2,\"max\":22}";
    j += ",{\"k\":\"sw\",\"v\":\"0x" + syncHex + "\",\"type\":\"hex\"}";
    j += ",{\"k\":\"preamble\",\"v\":" + String(cfg.preambleLength) + ",\"opts\":\"8|12|16|22|32\",\"min\":6,\"max\":65535}";
    j += ",{\"k\":\"tcxo\",\"v\":" + String(cfg.tcxoVoltage, 1) + ",\"unit\":\"V\",\"opts\":\"0.0|1.6|1.8|2.4|3.3\"}";
    j += ",{\"k\":\"dio2\",\"v\":" + String(cfg.useDio2AsRfSwitch ? 1 : 0) + ",\"opts\":\"0|1\"}";
    j += ",{\"k\":\"sclk\",\"v\":" + String(cfg.sclkPin) + ",\"type\":\"pin\"}";
    j += ",{\"k\":\"miso\",\"v\":" + String(cfg.misoPin) + ",\"type\":\"pin\"}";
    j += ",{\"k\":\"mosi\",\"v\":" + String(cfg.mosiPin) + ",\"type\":\"pin\"}";
    j += ",{\"k\":\"nss\",\"v\":" + String(cfg.nssPin) + ",\"type\":\"pin\"}";
    j += ",{\"k\":\"rst\",\"v\":" + String(cfg.rstPin) + ",\"type\":\"pin\"}";
    j += ",{\"k\":\"dio0\",\"v\":" + String(cfg.dio0Pin) + ",\"type\":\"pin\"}";
    j += ",{\"k\":\"dio1\",\"v\":" + String(cfg.dio1Pin) + ",\"type\":\"pin\"}";
    j += ",{\"k\":\"weather\",\"v\":" + String(weatherModeEnabled ? 1 : 0) + ",\"opts\":\"0|1\"}";
    j += ",{\"k\":\"weather_interval\",\"v\":" + String(weatherIntervalMs) + ",\"unit\":\"ms\",\"opts\":\"5000|10000|30000|60000|300000\",\"min\":500,\"max\":600000}";
    j += ",{\"k\":\"weather_sensors\",\"v\":" + String(sensorCount) + ",\"type\":\"count\"}";
    j += "],\"cmds\":\"setup|info|set|device|save|init|autostart|startmesh|reboot|bt off|mesh scan|mesh peers|mesh key|mesh keysend|mesh keygen|mesh enc|weather on|weather off|weather interval|weather add|weather clear|weather now|send|sendto\"";
    j += "}";
    sendMsg(j);
}

void startConfigMode()
{
    setupModeActive = true;
    setupSaveRequested = false;

    // 1. Announce config mode and send full setup info so the client immediately
    //    knows all current values, ranges, and available commands.
    sendMsg("{\"evt\":\"config_mode\"}");
    sendMsg("{\"evt\":\"serial_setup_ready\"}");
    sendSetupInfo();

    // 2. Interactive loop: keep processing commands until the user has explicitly
    //    saved the config AND the radio has been initialised successfully.
    //    We send a reminder every ~5 s so that newly connected BLE clients also
    //    receive the prompt without needing to ask for it.
    unsigned long lastPrompt = millis();
    const unsigned long PROMPT_INTERVAL = 5000;

    while (true)
    {
        checkInputs();

        if (setupSaveRequested && radioInitialized)
            break;

        // Re-send a lightweight status reminder so clients can track progress.
        if (millis() - lastPrompt >= PROMPT_INTERVAL)
        {
            lastPrompt = millis();
            String status = "{\"evt\":\"cfg_status\"";
            status += ",\"setup_active\":true";
            status += ",\"saved\":" + String(setupSaveRequested ? "true" : "false");
            status += ",\"radio_ok\":" + String(radioInitialized ? "true" : "false");
            // If anything changed, include updated field snapshots
            if (!setupSaveRequested)
                status += ",\"hint\":\"send save when all fields are correct\"";
            else if (!radioInitialized)
                status += ",\"hint\":\"send init to test radio with current config\"";
            status += "}";
            sendMsg(status);
        }

        delay(20);
    }

    // 3. Configuration complete — confirm and start mesh.
    saveConfig();
    sendMsg("{\"evt\":\"config_done\"}");
    uint64_t mac = ESP.getEfuseMac();
    nodeId = (uint16_t)(mac & 0xFFFF);
    meshRunning = true;
    sendMsg(String("{\"evt\":\"mesh_started\",\"nodeId\":") + nodeId + "}");
    setupModeActive = false;
}

void setup()
{
    Serial.begin(115200);

    loadConfig();

    cfg.btEnabled = true;
    enableBluetoothVisible("ESP32-LoRaCfg");
    sendMsg("{\"evt\":\"boot\"}");

    // try to load
    if (!configSaved)
    {
        sendMsg("{\"evt\":\"first_boot\"}");

        setDefaultsHeltec();

        if (!btActive)
        {
            enableBluetoothVisible("ESP32-LoRaCfg");
        }
        startConfigMode();
    }
    else
    {
        sendMsg("{\"evt\":\"cfg_loaded\"}");
        sendMsg("{\"evt\":\"setup_done\",\"persisted\":true}");
        sendConfig();
        sendSetupInfo();
        sendMsg("{\"evt\":\"setup_available\",\"cmd\":\"setup\"}");
    }
}

static uint16_t crc16_ccitt(const uint8_t *data, size_t len)
{
    uint16_t crc = 0xFFFF;
    while (len--)
    {
        crc ^= (uint16_t)(*data++) << 8;
        for (uint8_t i = 0; i < 8; i++)
        {
            if (crc & 0x8000)
                crc = (crc << 1) ^ 0x1021;
            else
                crc <<= 1;
        }
    }
    return crc;
}

// very small duplicate cache
#define DUP_CACHE_SIZE 24
static uint16_t dup_cache[DUP_CACHE_SIZE];
static uint8_t dup_head = 0;

bool isDuplicate(uint16_t crc)
{
    for (uint8_t i = 0; i < DUP_CACHE_SIZE; i++)
        if (dup_cache[i] == crc && crc != 0)
            return true;
    // insert
    dup_cache[dup_head++] = crc;
    if (dup_head >= DUP_CACHE_SIZE)
        dup_head = 0;
    return false;
}

String toHex(const uint8_t *buf, size_t len)
{
    String s;
    s.reserve(len * 2 + 4);
    for (size_t i = 0; i < len; i++)
    {
        uint8_t hi = (buf[i] >> 4) & 0x0F;
        uint8_t lo = buf[i] & 0x0F;
        s += (char)(hi < 10 ? '0' + hi : 'A' + hi - 10);
        s += (char)(lo < 10 ? '0' + lo : 'A' + lo - 10);
    }
    return s;
}

void handleIncoming(uint8_t *buf, size_t len)
{
    uint16_t crc = crc16_ccitt(buf, len);
    if (isDuplicate(crc))
    {
        // ignore duplicates
        return;
    }
    String j = "{";
    j += "\"evt\":\"rx\",";
    j += "\"len\":" + String(len) + ",";
    j += "\"data\":\"" + toHex(buf, len) + "\"";
    j += "}";
    sendMsg(j);

    handleMeshFrame(buf, len);
}

void loop()
{
    checkInputs();

    if (meshRunning && radioInitialized && radioPtr)
    {
        uint8_t buf[256];

        int16_t state = radioPtr->receive(buf, sizeof(buf), 100);
        if (state > 0)
        {

            size_t received = (size_t)state;
            handleIncoming(buf, received);
        }
        else if (state == RADIOLIB_ERR_NONE)
        {
        }
    }

    if (meshRunning && weatherModeEnabled)
    {
        uint32_t nowMs = millis();
        if (nowMs - lastWeatherTxMs >= weatherIntervalMs)
        {
            lastWeatherTxMs = nowMs;
            sendWeatherPacket();
        }
    }

    delay(20);
}
