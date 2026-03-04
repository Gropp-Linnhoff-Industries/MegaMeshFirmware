////////////////////////////////////////////////////////////////////////////////////
// This code is primaraly designed for educational purpose as a school Projekt    //
// ------------------------------------------------------------------------------ //
// The current configuration is optimised for Heltec Lora 32 v3(3.2)              //
// ------------------------------------------------------------------------------ //
// Authers:                                                                       //
// Flavius Linnhoff   @https://github.com/Flavours64                              //
// Benedict Gropp     @https://github.com/Benemaster                              //
// ------------------------------------------------------------------------------ //
// Project Name: MegaMesh                                                         //
// GitHub Projekt Page:                                                           //
// GitHub Web Client:                                                             //
// ------------------------------------------------------------------------------ //
// Basic explenation of the project:                                              //
//                                                                                //
// This project trys to create a indipendent scaleble meshcommunication network   //
// for weather data and many other data types. Key features are security and      //
// scalebility. It supports multiple options for useres to use and interact with  //
// this firmware via spezific hardware or with every Chrome or Edge Browser or    //
// an AndroidApp.                                                                 //
//                                                                                //
// ------------------------------------------------------------------------------ //
// !!! This is only a small part of the project and it will not be easy to use    //
// !!! setup. There is no setup instruction or dokumentation avalible.            //
//                                                                                //
////////////////////////////////////////////////////////////////////////////////////
}

void printPeerKeys()
#include <SPI.h> // SPI library for communication with the LoRa module
    out.println("Gespeicherte Node-Keys:");
bool any = false;
for (uint8_t i = 0; i < KEY_CACHE_SIZE; i++)
{
    if (!peerKeys[i].valid)
        continue;
    any = true;
    pf("- node=0x%X key=%s\n", peerKeys[i].node, keyToHex(peerKeys[i].key).c_str());
}
if (!any)
    out.println("(keine)");
}
#include <BLEDevice.h> // BLE library for Bluetooth communication
#include <BLEServer.h> // BLE server library for handling Bluetooth server functionality
#include <BLE2902.h>   // BLE descriptor library for handling Bluetooth descriptors

// Additional headers to satisfy IntelliSense and provide runtime APIs
#include <stdarg.h>    // va_list, vsnprintf used by pf()
#include <esp_sleep.h> // deep sleep helpers (ESP32)

// Pin definitions for the LoRa module
static const uint8_t PIN_NSS = 8;
static const uint8_t PIN_SCK = 9;
static const uint8_t PIN_MOSI = 10;
static const uint8_t PIN_MISO = 11;
static const uint8_t PIN_RST = 12;
static const uint8_t PIN_BUSY = 13;
static const uint8_t PIN_DIO1 = 14;
static const uint8_t PIN_VBAT_READ = 1;  // ADC for battery voltage (Heltec V3)
static const uint8_t PIN_VBAT_CTRL = 37; // enable battery voltage ADC
static const uint8_t PIN_LED = 35;       // onboard LED

// LoRa configuration parameters
static const float LORA_FREQUENCY = 868.0;
static const float LORA_BANDWIDTH = 125.0;
static const uint8_t LORA_SF = 9;
static const uint8_t LORA_CR = 7;
static const uint8_t LORA_SYNC_WORD = 0x12;
static const int8_t LORA_POWER = 17;
static const uint16_t LORA_PREAMBLE = 8;
static const float LORA_TCXO_VOLTAGE = 1.6;

// Mesh network configuration parameters
static const uint16_t MESH_MAGIC = 0x4D48;
static const uint8_t MESH_VERSION = 1;
static const uint16_t MESH_BROADCAST = 0xFFFF;
static const size_t MAX_MESH_PAYLOAD = 180;
static const size_t MAX_PACKET_SIZE = 220;
static const uint8_t DEFAULT_MAX_HOPS = 7;
static const uint8_t SEEN_CACHE_SIZE = 64;
static const uint8_t STATION_CACHE_SIZE = 32;
static const uint8_t KEY_CACHE_SIZE = 24;
static const uint8_t KEY_BYTES = 16;
static const uint8_t MESH_FLAG_ENCRYPTED = 0x01;

// Default public channel key
// channel works out of the box 32 hex = 16 bytes
static const uint8_t PUBLIC_KEY[KEY_BYTES] = {
    0x4D, 0x45, 0x47, 0x41, 0x4D, 0x45, 0x53, 0x48,
    0x50, 0x55, 0x42, 0x4C, 0x49, 0x43, 0x30, 0x31}; // "MEGAMESHPUBLIC01"
static const char *CTRL_DISC_REQ = "#MESH_DISC_REQ";
static const char *CTRL_DISC_RESP = "#MESH_DISC_RESP";
static const char *CTRL_WX_REQ = "#MESH_WX_REQ";
static const char *CTRL_WX_DATA = "#MESH_WX_DATA";
static const char *CTRL_ACK = "#MESH_ACK";
static const char *CTRL_TRACE_REQ = "#MESH_TRACE_REQ";
static const char *CTRL_TRACE_RESP = "#MESH_TRACE_RESP";

// Reliable send configuration
static const uint8_t OUTBOUND_BUFFER_SIZE = 8;
static const uint8_t MAX_RETRIES = 10;
static const uint32_t RETRY_INTERVAL_MS = 5000;

// Data structures for mesh network management
struct SeenEntry
{
    uint16_t origin;
    uint16_t msgId;
    uint32_t seenAt;
};

// Data structure for storing information about stations in the mesh network
struct StationEntry
{
    uint16_t node;
    uint32_t lastSeen;
    float rssi;
    float snr;
    uint8_t hops;
};

// Data structure for storing peer keys for encryption
struct PeerKeyEntry
{
    bool valid;
    uint16_t node;
    uint8_t key[KEY_BYTES];
};

// Data structure for outbound message buffer (reliable send)
struct OutboundEntry
{
    bool active;
    MeshHeader header;
    uint8_t payload[MAX_MESH_PAYLOAD];
    uint8_t retries;
    uint32_t lastSentAt;
};

// Data structure for the mesh packet header
#pragma pack(push, 1)
struct MeshHeader
{
    uint16_t magic;
    uint8_t version;
    uint16_t origin;
    uint16_t msgId;
    uint16_t destination;
    uint8_t hopCount;
    uint8_t maxHops;
    uint8_t flags;
    uint8_t payloadLen;
};
#pragma pack(pop)

// RadioLib setup for the LoRa module
Module radioModule(PIN_NSS, PIN_DIO1, PIN_RST, PIN_BUSY);
SX1262 radio(&radioModule);

// Global variables for managing the mesh network state
volatile bool radioIrq = false;
uint16_t nodeId = 0;
uint16_t nextMsgId = 1;
uint8_t configuredMaxHops = DEFAULT_MAX_HOPS;
bool weatherModeEnabled = false;
String serialLine;
SeenEntry seenCache[SEEN_CACHE_SIZE];
uint8_t seenWritePos = 0;
StationEntry stations[STATION_CACHE_SIZE];
PeerKeyEntry peerKeys[KEY_CACHE_SIZE];
bool personalKeyValid = false;
uint8_t personalKey[KEY_BYTES] = {0};

// Reliable send buffer
OutboundEntry outboundBuffer[OUTBOUND_BUFFER_SIZE];
bool reliableSendEnabled = true;

// Sleep mode
bool sleepEnabled = false;
uint32_t sleepIdleMs = 60000; // auto-sleep after 60s idle
uint32_t lastActivityMs = 0;

// Weather station location
float wxLatitude = 0.0;
float wxLongitude = 0.0;
bool wxLocationSet = false;

// Bluetooth characteristic
BLECharacteristic *pTxChar = nullptr;
bool bleConnected = false;

// BLE command ring buffer
#define BLE_CMD_QUEUE_SIZE 16
String bleCmdQueue[BLE_CMD_QUEUE_SIZE];
volatile uint8_t bleCmdHead = 0;
volatile uint8_t bleCmdTail = 0;

// dual output class for serial and BLE for shorter code
class DualPrint : public Print
{
    String _bleBuf;

public:
    size_t write(uint8_t c) override
    {
        Serial.write(c);
        if (bleConnected && pTxChar)
        {
            _bleBuf += (char)c;
            if (c == '\n')
                flushBLE();
        }
        return 1;
    }
    size_t write(const uint8_t *buf, size_t len) override
    {
        Serial.write(buf, len);
        if (bleConnected && pTxChar)
            for (size_t i = 0; i < len; i++)
            {
                _bleBuf += (char)buf[i];
                if (buf[i] == '\n')
                    flushBLE();
            }
        return len;
    }
    void flushBLE()
    {
        while (_bleBuf.length() > 0)
        {
            size_t n = _bleBuf.length() < 200 ? _bleBuf.length() : 200;
            pTxChar->setValue((uint8_t *)_bleBuf.c_str(), n);
            pTxChar->notify();
            _bleBuf = _bleBuf.substring(n);
            delay(5);
        }
    }
};
DualPrint out;

// formatted print helper – replaces many multi-line out.print() chains
void pf(const char *fmt, ...)
{
    char buf[300];
    va_list args;
    va_start(args, fmt);
    vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    out.print(buf);
}
void resetIdleTimer() { lastActivityMs = millis(); }

class MeshBLEServerCB : public BLEServerCallbacks
{
    void onConnect(BLEServer *s) override { bleConnected = true; }
    void onDisconnect(BLEServer *s) override
    {
        bleConnected = false;
        BLEDevice::startAdvertising();
    }
};

class MeshBLERxCB : public BLECharacteristicCallbacks
{
    void onWrite(BLECharacteristic *c) override
    {
        String v = String(c->getValue().c_str());
        v.trim();
        if (v.length() > 0)
        {
            uint8_t nextHead = (bleCmdHead + 1) % BLE_CMD_QUEUE_SIZE;
            if (nextHead != bleCmdTail) // queue not full
            {
                bleCmdQueue[bleCmdHead] = v;
                bleCmdHead = nextHead;
            }
        }
    }
};

bool parseNodeValue(const String &input, uint16_t &outNode)
{
    String raw = input;
    raw.trim();
    if (raw.length() == 0)
    {
        return false;
    }

    char *endPtr = nullptr;
    unsigned long parsed = strtoul(raw.c_str(), &endPtr, 0);
    if (endPtr == raw.c_str() || *endPtr != '\0' || parsed > 0xFFFF)
    {
        return false;
    }

    outNode = static_cast<uint16_t>(parsed);
    return true;
}

int hexNibble(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return 10 + (c - 'a');
    if (c >= 'A' && c <= 'F')
        return 10 + (c - 'A');
    return -1;
}

String keyToHex(const uint8_t *key)
{
    const char *digits = "0123456789ABCDEF";
    String out;
    out.reserve(KEY_BYTES * 2);
    for (uint8_t i = 0; i < KEY_BYTES; i++)
    {
        out += digits[(key[i] >> 4) & 0x0F];
        out += digits[key[i] & 0x0F];
    }
    return out;
}

bool parseHexKey(String raw, uint8_t *outKey)
{
    raw.trim();
    if (raw.startsWith("0x") || raw.startsWith("0X"))
    {
        raw = raw.substring(2);
    }

    if (raw.length() != KEY_BYTES * 2)
    {
        return false;
    }

    for (uint8_t i = 0; i < KEY_BYTES; i++)
    {
        int hi = hexNibble(raw[i * 2]);
        int lo = hexNibble(raw[i * 2 + 1]);
        if (hi < 0 || lo < 0)
        {
            return false;
        }
        outKey[i] = static_cast<uint8_t>((hi << 4) | lo);
    }

    return true;
}

void generatePersonalKey()
{
    for (uint8_t i = 0; i < KEY_BYTES; i++)
    {
        personalKey[i] = static_cast<uint8_t>(random(0, 256));
    }
    personalKeyValid = true;
}

int findPeerKeyIndex(uint16_t node)
{
    for (uint8_t i = 0; i < KEY_CACHE_SIZE; i++)
    {
        if (peerKeys[i].valid && peerKeys[i].node == node)
        {
            return i;
        }
    }
    return -1;
}

int findPeerKeyFreeSlot()
{
    for (uint8_t i = 0; i < KEY_CACHE_SIZE; i++)
    {
        if (!peerKeys[i].valid)
        {
            return i;
        }
    }
    return -1;
}

bool setPeerKey(uint16_t node, const uint8_t *key)
{
    int index = findPeerKeyIndex(node);
    if (index < 0)
    {
        index = findPeerKeyFreeSlot();
    }
    if (index < 0)
    {
        return false;
    }

    peerKeys[index].valid = true;
    peerKeys[index].node = node;
    memcpy(peerKeys[index].key, key, KEY_BYTES);
    return true;
}

bool deletePeerKey(uint16_t node)
{
    int index = findPeerKeyIndex(node);
    if (index < 0)
    {
        return false;
    }

    peerKeys[index].valid = false;
    peerKeys[index].node = 0;
    memset(peerKeys[index].key, 0, KEY_BYTES);
    return true;
}

void printPeerKeys()
{\n    out.println(\"Gespeicherte Node-Keys:\");\n    bool any = false;\n    for (uint8_t i = 0; i < KEY_CACHE_SIZE; i++) {\n        if (!peerKeys[i].valid) continue;\n        any = true;\n        pf(\"- node=0x%X key=%s\\n\", peerKeys[i].node, keyToHex(peerKeys[i].key).c_str());\n    }\n    if (!any) out.println(\"(keine)\");\n}

void cryptPayload(uint8_t *buffer, size_t len, const uint8_t *key, const MeshHeader &header)
{
        // Initialize the AES context
        mbedtls_aes_context aes;
        mbedtls_aes_init(&aes);

        // AES key setup (128)
        mbedtls_aes_setkey_enc(&aes, key, 128);

        uint8_t iv[16] = {0};

        iv[0] = (header.origin >> 8) & 0xFF;
        iv[1] = header.origin & 0xFF;
        iv[2] = (header.destination >> 8) & 0xFF;
        iv[3] = header.destination & 0xFF;
        iv[4] = (header.msgId >> 8) & 0xFF;
        iv[5] = header.msgId & 0xFF;

        // Variables for mbedtls
        size_t nc_off = 0;
        uint8_t stream_block[16] = {0};

        // CTR mode
        mbedtls_aes_crypt_ctr(&aes, len, &nc_off, iv, stream_block, buffer, buffer);

        // clean to prevent memory leaks
        mbedtls_aes_free(&aes);
}

int findStationIndex(uint16_t node)
{
        for (uint8_t index = 0; index < STATION_CACHE_SIZE; index++)
        {
            if (stations[index].node == node)
            {
                return index;
            }
        }
        return -1;
}

void updateStation(uint16_t node, float rssi, float snr, uint8_t hops)
{
        int slot = findStationIndex(node);
        if (slot < 0)
        {
            for (uint8_t index = 0; index < STATION_CACHE_SIZE; index++)
            {
                if (stations[index].node == 0)
                {
                    slot = index;
                    break;
                }
            }
        }

        if (slot < 0)
        {
            uint8_t oldest = 0;
            for (uint8_t index = 1; index < STATION_CACHE_SIZE; index++)
            {
                if (stations[index].lastSeen < stations[oldest].lastSeen)
                {
                    oldest = index;
                }
            }
            slot = oldest;
        }

        stations[slot].node = node;
        stations[slot].lastSeen = millis();
        stations[slot].rssi = rssi;
        stations[slot].snr = snr;
        stations[slot].hops = hops;
}

void printStations()
{
        out.println("Gefundene Stationen:");
        bool any = false;
        uint32_t nowMs = millis();
        for (uint8_t i = 0; i < STATION_CACHE_SIZE; i++)
        {
            if (stations[i].node == 0)
                continue;
            any = true;
            pf("- 0x%X last=%lus rssi=%.0f snr=%.1f hops=%u\n",
               stations[i].node, (nowMs - stations[i].lastSeen) / 1000UL,
               stations[i].rssi, stations[i].snr, stations[i].hops);
        }
        if (!any)
            out.println("(keine)");
}

#if defined(ESP8266) || defined(ESP32)
ICACHE_RAM_ATTR
#endif
void setRadioFlag(void)
{
        radioIrq = true;
}

bool wasSeen(uint16_t origin, uint16_t msgId)
{
        const uint32_t nowMs = millis();
        for (uint8_t index = 0; index < SEEN_CACHE_SIZE; index++)
        {
            if (seenCache[index].origin == origin && seenCache[index].msgId == msgId)
            {
                if (nowMs - seenCache[index].seenAt < 120000UL)
                {
                    return true;
                }
            }
        }
        return false;
}

void rememberSeen(uint16_t origin, uint16_t msgId)
{
        seenCache[seenWritePos].origin = origin;
        seenCache[seenWritePos].msgId = msgId;
        seenCache[seenWritePos].seenAt = millis();
        seenWritePos = (seenWritePos + 1) % SEEN_CACHE_SIZE;
}

void restartReceive()
{
        radioIrq = false;
        int16_t state = radio.startReceive();
        if (state != RADIOLIB_ERR_NONE)
        {
            out.print("RX start error: ");
            out.println(state);
        }
}

bool sendMeshFrame(MeshHeader &header, const uint8_t *payload)
{
        const size_t totalLen = sizeof(MeshHeader) + header.payloadLen;
        if (totalLen > MAX_PACKET_SIZE)
        {
            return false;
        }

        uint8_t frame[MAX_PACKET_SIZE];
        memcpy(frame, &header, sizeof(MeshHeader));
        if (header.payloadLen > 0)
        {
            memcpy(frame + sizeof(MeshHeader), payload, header.payloadLen);
        }

        int16_t txState = radio.transmit(frame, totalLen);
        if (txState != RADIOLIB_ERR_NONE)
        {
            out.print("TX error: ");
            out.println(txState);
            restartReceive();
            return false;
        }

        restartReceive();
        return true;
}

// ── Outbound buffer helpers (reliable send) ──────────────────────────

int findFreeOutboundSlot()
{
        for (uint8_t i = 0; i < OUTBOUND_BUFFER_SIZE; i++)
        {
            if (!outboundBuffer[i].active)
                return i;
        }
        return -1;
}

bool bufferOutboundMessage(const MeshHeader &hdr, const uint8_t *payload)
{
        int slot = findFreeOutboundSlot();
        if (slot < 0)
        {
            out.println("Outbound buffer full – message sent without ACK tracking.");
            return false;
        }
        outboundBuffer[slot].active = true;
        outboundBuffer[slot].header = hdr;
        memcpy(outboundBuffer[slot].payload, payload, hdr.payloadLen);
        outboundBuffer[slot].retries = 0;
        outboundBuffer[slot].lastSentAt = millis();
        return true;
}

void removeOutboundByMsgId(uint16_t origin, uint16_t msgId)
{
        for (uint8_t i = 0; i < OUTBOUND_BUFFER_SIZE; i++)
        {
            if (outboundBuffer[i].active &&
                outboundBuffer[i].header.origin == origin &&
                outboundBuffer[i].header.msgId == msgId)
            {
                outboundBuffer[i].active = false;
                out.print("ACK received – msgId=");
                out.print(msgId);
                out.println(" removed from buffer.");
                return;
            }
        }
}

void processOutboundBuffer()
{
        if (!reliableSendEnabled)
            return;

        uint32_t now = millis();
        for (uint8_t i = 0; i < OUTBOUND_BUFFER_SIZE; i++)
        {
            if (!outboundBuffer[i].active)
                continue;

            if (now - outboundBuffer[i].lastSentAt < RETRY_INTERVAL_MS)
                continue;

            outboundBuffer[i].retries++;
            if (outboundBuffer[i].retries > MAX_RETRIES)
            {
                pf("FAILED msgId=%u to=0x%X – no ACK after %u retries, dropped.\n",
                   outboundBuffer[i].header.msgId, outboundBuffer[i].header.destination, MAX_RETRIES);
                outboundBuffer[i].active = false;
                continue;
            }

            pf("RETRY #%u msgId=%u to=0x%X\n",
               outboundBuffer[i].retries, outboundBuffer[i].header.msgId, outboundBuffer[i].header.destination);

            sendMeshFrame(outboundBuffer[i].header, outboundBuffer[i].payload);
            outboundBuffer[i].lastSentAt = now;
        }
}

void printOutboundBuffer()
{
        out.println("Outbound buffer:");
        bool any = false;
        for (uint8_t i = 0; i < OUTBOUND_BUFFER_SIZE; i++)
        {
            if (!outboundBuffer[i].active)
                continue;
            any = true;
            pf("- msgId=%u to=0x%X retries=%u/%u\n",
               outboundBuffer[i].header.msgId, outboundBuffer[i].header.destination,
               outboundBuffer[i].retries, MAX_RETRIES);
        }
        if (!any)
            out.println("(empty)");
}

// ── Battery voltage (Heltec V3) ──────────────────────────────────────

float readBatteryVoltage()
{
        pinMode(PIN_VBAT_CTRL, OUTPUT);
        digitalWrite(PIN_VBAT_CTRL, HIGH);
        delay(10);
        uint32_t raw = 0;
        for (uint8_t i = 0; i < 8; i++)
            raw += analogRead(PIN_VBAT_READ);
        raw /= 8;
        digitalWrite(PIN_VBAT_CTRL, LOW);
        return (raw / 4095.0f) * 3.3f * 2.0f; // voltage divider ×2
}

void printBattery()
{
        float v = readBatteryVoltage();
        int pct = constrain((int)((v - 3.0f) / (4.2f - 3.0f) * 100), 0, 100);
        pf("Battery: %.2fV  %d%%\n", v, pct);
}

// ── Sleep mode (DIO1 wake on LoRa packet) ────────────────────────────

void enterSleep()
{
        pf("Entering deep sleep (wake on LoRa DIO1 = GPIO%d)...\n", PIN_DIO1);
        out.println("ZZZ");
        delay(100);
        restartReceive(); // radio must be in RX so DIO1 fires on packet
        delay(50);
        esp_sleep_enable_ext1_wakeup(1ULL << PIN_DIO1, ESP_EXT1_WAKEUP_ANY_HIGH);
        esp_deep_sleep_start();
}

// ── ACK helper ───────────────────────────────────────────────────────

void sendAck(uint16_t destination, uint16_t ackedOrigin, uint16_t ackedMsgId)
{
        String payload = String(CTRL_ACK) + ":" +
                         String(ackedOrigin, HEX) + ":" +
                         String(ackedMsgId);
        sendTextTo(destination, payload);
}

// ── Trace route helpers ──────────────────────────────────────────────

void sendTraceRoute(uint16_t destination)
{
        if (destination == MESH_BROADCAST)
        {
            out.println("Traceroute requires a specific node ID.");
            return;
        }
        // Payload: "#MESH_TRACE_REQ:0xOrigin"
        String payload = String(CTRL_TRACE_REQ) + ":0x" + String(nodeId, HEX);
        if (sendTextTo(destination, payload))
        {
            out.print("TRACEROUTE sent to 0x");
            out.println(destination, HEX);
        }
}

bool sendTextTo(uint16_t destination, const String &text, bool encrypted = false)
{
        if (text.length() == 0)
        {
            return false;
        }

        String payloadText = text;
        if (payloadText.length() > MAX_MESH_PAYLOAD)
        {
            payloadText = payloadText.substring(0, MAX_MESH_PAYLOAD);
        }

        MeshHeader header;
        header.magic = MESH_MAGIC;
        header.version = MESH_VERSION;
        header.origin = nodeId;
        header.msgId = nextMsgId++;
        header.destination = destination;
        header.hopCount = 0;
        header.maxHops = configuredMaxHops;
        header.flags = encrypted ? MESH_FLAG_ENCRYPTED : 0;
        header.payloadLen = static_cast<uint8_t>(payloadText.length());

        uint8_t payloadBytes[MAX_MESH_PAYLOAD];
        memcpy(payloadBytes, payloadText.c_str(), header.payloadLen);
        if (encrypted)
        {
            if (!personalKeyValid)
            {
                out.println("Kein eigener Key. Nutze: /mykey gen");
                return false;
            }
            cryptPayload(payloadBytes, header.payloadLen, personalKey, header);
        }

        rememberSeen(header.origin, header.msgId);
        bool sent = sendMeshFrame(header, payloadBytes);

        // Buffer for reliable delivery if enabled, directed, and not a control/ACK message
        if (sent && reliableSendEnabled &&
            destination != MESH_BROADCAST &&
            !payloadText.startsWith("#MESH_"))
        {
            bufferOutboundMessage(header, payloadBytes);
        }

        return sent;
}

void sendDiscoveryRequest()
{
        if (sendTextTo(MESH_BROADCAST, String(CTRL_DISC_REQ)))
        {
            out.println("SCAN gesendet: warte auf Antworten...");
        }
}

void sendDiscoveryResponse(uint16_t destination)
{
        String payload = String(CTRL_DISC_RESP) + ":" + String(nodeId, HEX);
        sendTextTo(destination, payload);
}

String weatherInfo()
{
        float temperature = 21.5f;
        float humidity = 48.0f;
        float pressure = 1012.8f;

        String payload = String(CTRL_WX_DATA);
        payload += ":node=0x";
        payload += String(nodeId, HEX);
        payload += ",tempC=";
        payload += String(temperature, 1);
        payload += ",hum=";
        payload += String(humidity, 1);
        payload += ",hPa=";
        payload += String(pressure, 1);
        if (wxLocationSet)
        {
            payload += ",lat=";
            payload += String(wxLatitude, 6);
            payload += ",lon=";
            payload += String(wxLongitude, 6);
        }
        return payload;
}

void sendWeatherRequest(uint16_t dest)
{
        if (sendTextTo(dest, String(CTRL_WX_REQ)))
            pf("WX request gesendet an %s\n", dest == MESH_BROADCAST ? "broadcast" : ("0x" + String(dest, HEX)).c_str());
}

void sendWeatherResponse(uint16_t dest)
{
        if (sendTextTo(dest, weatherInfo()))
            pf("WX response gesendet an 0x%X\n", dest);
}

// unified send: broadcast, direct, or encrypted direct
void sendMsg(uint16_t dest, const String &text, bool encrypted = false)
{
        if (text.length() == 0)
            return;
        if (encrypted && dest == MESH_BROADCAST)
        {
            out.println("Verschluesselt nur als Direktnachricht erlaubt.");
            return;
        }
        String t = text.substring(0, MAX_MESH_PAYLOAD);
        if (sendTextTo(dest, t, encrypted))
        {
            uint16_t mid = nextMsgId - 1;
            if (dest == MESH_BROADCAST)
                pf("TX msgId=%u hops=0/%u text=%s\n", mid, configuredMaxHops, t.c_str());
            else
                pf("%sTX to=0x%X msgId=%u text=%s\n", encrypted ? "E" : "", dest, mid, t.c_str());
        }
}

void sendPublicMessage(const String &message)
{
        if (message.length() == 0)
        {
            return;
        }

        String payloadText = message;
        if (payloadText.length() > MAX_MESH_PAYLOAD)
        {
            payloadText = payloadText.substring(0, MAX_MESH_PAYLOAD);
        }

        MeshHeader header;
        header.magic = MESH_MAGIC;
        header.version = MESH_VERSION;
        header.origin = nodeId;
        header.msgId = nextMsgId++;
        header.destination = MESH_BROADCAST;
        header.hopCount = 0;
        header.maxHops = configuredMaxHops;
        header.flags = MESH_FLAG_ENCRYPTED;
        header.payloadLen = static_cast<uint8_t>(payloadText.length());

        uint8_t payloadBytes[MAX_MESH_PAYLOAD];
        memcpy(payloadBytes, payloadText.c_str(), header.payloadLen);
        cryptPayload(payloadBytes, header.payloadLen, PUBLIC_KEY, header);

        rememberSeen(header.origin, header.msgId);
        if (sendMeshFrame(header, payloadBytes))
            pf("TX msgId=%u hops=0/%u enc=pub text=%s\n", (uint16_t)(nextMsgId - 1), configuredMaxHops, payloadText.c_str());
}

void relayIfNeeded(const MeshHeader &incoming, const uint8_t *payload, bool alreadySeen, const char *decodedText = nullptr)
{
        if (alreadySeen)
        {
            return;
        }
        if (incoming.hopCount >= incoming.maxHops)
        {
            return;
        }

        MeshHeader relay = incoming;
        relay.hopCount = incoming.hopCount + 1;
        rememberSeen(relay.origin, relay.msgId);

        // Special handling: append our node ID to trace route requests before relaying
        if (decodedText != nullptr)
        {
            String txt(decodedText);
            if (txt.startsWith(String(CTRL_TRACE_REQ)))
            {
                // Append our nodeId to the route path
                String newPayload = txt + ">0x" + String(nodeId, HEX);
                if (newPayload.length() <= MAX_MESH_PAYLOAD)
                {
                    relay.payloadLen = static_cast<uint8_t>(newPayload.length());
                    uint8_t modifiedPayload[MAX_MESH_PAYLOAD];
                    memcpy(modifiedPayload, newPayload.c_str(), relay.payloadLen);
                    if (sendMeshFrame(relay, modifiedPayload))
                        pf("RELAY TRACE origin=0x%X hops=%u/%u\n", relay.origin, relay.hopCount, relay.maxHops);
                    return;
                }
            }
        }

        if (sendMeshFrame(relay, payload))
            pf("RELAY origin=0x%X msgId=%u hops=%u/%u\n", relay.origin, relay.msgId, relay.hopCount, relay.maxHops);
}

void handleReceivedPacket()
{
        const size_t packetLen = radio.getPacketLength();
        if (packetLen < sizeof(MeshHeader) || packetLen > MAX_PACKET_SIZE)
        {
            restartReceive();
            return;
        }

        uint8_t frame[MAX_PACKET_SIZE];
        int16_t state = radio.readData(frame, packetLen);
        if (state != RADIOLIB_ERR_NONE)
        {
            out.print("RX error: ");
            out.println(state);
            restartReceive();
            return;
        }

        MeshHeader header;
        memcpy(&header, frame, sizeof(MeshHeader));
        if (header.magic != MESH_MAGIC || header.version != MESH_VERSION)
        {
            restartReceive();
            return;
        }

        const size_t announcedPayloadLen = header.payloadLen;
        const size_t actualPayloadLen = packetLen - sizeof(MeshHeader);
        const size_t payloadLen = announcedPayloadLen < actualPayloadLen ? announcedPayloadLen : actualPayloadLen;
        const uint8_t *payloadPtr = frame + sizeof(MeshHeader);
        bool alreadySeen = wasSeen(header.origin, header.msgId);
        float rssi = radio.getRSSI();
        float snr = radio.getSNR();

        if (!alreadySeen)
        {
            rememberSeen(header.origin, header.msgId);
        }

        if (header.origin != nodeId)
        {
            updateStation(header.origin, rssi, snr, header.hopCount);
        }

        // Decode payload outside the destination check so relay can use it for trace route
        bool decryptedYeah = true;
        char textBuffer[MAX_MESH_PAYLOAD + 1];
        textBuffer[0] = '\0';
        const size_t copyLen = payloadLen <= MAX_MESH_PAYLOAD ? payloadLen : MAX_MESH_PAYLOAD;

        if (header.destination == MESH_BROADCAST || header.destination == nodeId)
        {
            bool encrypted = (header.flags & MESH_FLAG_ENCRYPTED) != 0;
            uint8_t payloadWork[MAX_MESH_PAYLOAD];

            memcpy(payloadWork, payloadPtr, copyLen);
            if (encrypted)
            {
                int keyIndex = findPeerKeyIndex(header.origin);
                if (keyIndex >= 0)
                {
                    // Decrypt with stored peer key
                    cryptPayload(payloadWork, copyLen, peerKeys[keyIndex].key, header);
                }
                else if (header.destination == MESH_BROADCAST)
                {
                    // Broadcast without peer key — try public key
                    cryptPayload(payloadWork, copyLen, PUBLIC_KEY, header);
                }
                else
                {
                    decryptedYeah = false;
                }
            }

            char textBuffer[MAX_MESH_PAYLOAD + 1];
            memcpy(textBuffer, payloadWork, copyLen);
            textBuffer[copyLen] = '\0';

            char destStr[12];
            if (header.destination == MESH_BROADCAST)
                strcpy(destStr, "bcast");
            else
                snprintf(destStr, sizeof(destStr), "0x%X", header.destination);
            pf("RX origin=0x%X dest=%s msgId=%u hops=%u/%u rssi=%.0f snr=%.1f enc=%d text=",
               header.origin, destStr, header.msgId, header.hopCount, header.maxHops, rssi, snr, encrypted ? 1 : 0);
            if (encrypted && !decryptedYeah)
                out.println("<encrypted: key fehlt fuer origin>");
            else
                out.println(textBuffer);

            String payloadText = decryptedYeah ? String(textBuffer) : String("");

            // ── Handle ACK messages ──────────────────────────────────────
            if (decryptedYeah && payloadText.startsWith(String(CTRL_ACK)) && header.destination == nodeId)
            {
                // Format: #MESH_ACK:<originHex>:<msgId>
                int firstColon = payloadText.indexOf(':', String(CTRL_ACK).length());
                if (firstColon > 0)
                {
                    int secondColon = payloadText.indexOf(':', firstColon + 1);
                    if (secondColon > 0)
                    {
                        String originStr = payloadText.substring(firstColon + 1, secondColon);
                        String msgIdStr = payloadText.substring(secondColon + 1);
                        uint16_t ackedOrigin = static_cast<uint16_t>(strtoul(originStr.c_str(), nullptr, 16));
                        uint16_t ackedMsgId = static_cast<uint16_t>(msgIdStr.toInt());
                        removeOutboundByMsgId(ackedOrigin, ackedMsgId);
                    }
                }
            }

            // ── Handle trace route request (we are the destination) ──────
            if (!alreadySeen && decryptedYeah && payloadText.startsWith(String(CTRL_TRACE_REQ)) && header.destination == nodeId && header.origin != nodeId)
            {
                // Append ourselves and send back as TRACE_RESP
                String route = payloadText.substring(String(CTRL_TRACE_REQ).length() + 1); // skip ':'
                route += ">0x" + String(nodeId, HEX);
                String respPayload = String(CTRL_TRACE_RESP) + ":" + route;
                sendTextTo(header.origin, respPayload);
                pf("TRACEROUTE arrived from 0x%X route=%s\n", header.origin, route.c_str());
            }

            // ── Handle trace route response (we are the origin) ─────────
            if (decryptedYeah && payloadText.startsWith(String(CTRL_TRACE_RESP)) && header.destination == nodeId)
            {
                String route = payloadText.substring(String(CTRL_TRACE_RESP).length() + 1);
                pf("TRACEROUTE to 0x%X: %s\n", header.origin, route.c_str());
            }

            // ── Handle discovery ─────────────────────────────────────────
            if (!alreadySeen && decryptedYeah && payloadText == CTRL_DISC_REQ && header.origin != nodeId)
            {
                sendDiscoveryResponse(header.origin);
            }
            else if (decryptedYeah && payloadText.startsWith(String(CTRL_DISC_RESP)) && header.origin != nodeId)
                pf("DISCOVERED station=0x%X hops=%u rssi=%.0f snr=%.1f\n", header.origin, header.hopCount, rssi, snr);

            // ── Handle weather ───────────────────────────────────────────
            if (!alreadySeen && decryptedYeah && payloadText == CTRL_WX_REQ && header.origin != nodeId && weatherModeEnabled)
            {
                sendWeatherResponse(header.origin);
            }
            else if (decryptedYeah && payloadText.startsWith(String(CTRL_WX_DATA)) && header.origin != nodeId)
                pf("WEATHER from=0x%X hops=%u data=%s\n", header.origin, header.hopCount, payloadText.c_str());

            // ── Auto-ACK for directed non-control user messages ─────────
            if (!alreadySeen && decryptedYeah && header.destination == nodeId &&
                header.origin != nodeId && !payloadText.startsWith("#MESH_"))
            {
                sendAck(header.origin, header.origin, header.msgId);
            }
        }
        else
        {
            // Packet is not for us, but we still need to decode unencrypted payloads
            // for trace route relay (appending our node ID)
            bool encrypted = (header.flags & MESH_FLAG_ENCRYPTED) != 0;
            if (!encrypted)
            {
                memcpy(textBuffer, payloadPtr, copyLen);
                textBuffer[copyLen] = '\0';
            }
            else
            {
                decryptedYeah = false;
            }
        }

        relayIfNeeded(header, payloadPtr, alreadySeen, decryptedYeah ? textBuffer : nullptr);
}

void printHelp()
{
        out.println("Mesh Serial Commands:");
        out.println("/help              -> Hilfe anzeigen");
        out.println("/id                -> eigene Node-ID");
        out.println("/ttl <1..15>       -> maxHops ");
        out.println("/scan              -> Stationen suchen");
        out.println("/stations          -> gefundene Stationen anzeigen");
        out.println("/msg <id> <text>   -> Direktnachricht (unverschluesselt)");
        out.println("/wx on|off|status  -> Weather-Mode steuern");
        out.println("/wxreq [all|node]  -> Wetterdaten anfragen");
        out.println("/wxloc <lat> <lon> -> Standort fuer Weather-Mode setzen");
        out.println("/wxloc show        -> aktuellen Standort anzeigen");
        out.println("/mykey gen|show|set-> eigener Key fuer eigene Node-ID");
        out.println("/key set <id> <hex32> -> Key fuer fremde Node-ID speichern");
        out.println("/key del <id>      -> Key fuer Node-ID loeschen");
        out.println("/keys              -> gespeicherte Keys anzeigen");
        out.println("/eto <id> <text>   -> verschluesselte Direktnachricht");
        out.println("/pub <text>        -> oeffentliche Nachricht (Public Key)");
        out.println("/traceroute <id>   -> Route zu einer Node anzeigen");
        out.println("/reliable on|off|status -> Zuverlaessiges Senden ein/aus");
        out.println("/buffer            -> Outbound-Buffer anzeigen");
        out.println("/battery           -> Batteriespannung anzeigen");
        out.println("/sleep             -> sofort in Deep-Sleep gehen");
        out.println("/autosleep on|off  -> Auto-Sleep ein/aus");
        out.println("/autosleep <sek>   -> Idle-Zeit bis Sleep setzen");
        out.println("/settings          -> alle Einstellungen anzeigen (JSON)");
        out.println("jede andere Zeile  -> als Mesh-Nachricht senden");
}

void handleSerialLine(String line)
{
        line.trim();
        if (line.length() == 0)
            return;
        resetIdleTimer();

        if (line == "/help")
        {
            printHelp();
            return;
        }

        if (line == "/settings")
        {
            float bv = readBatteryVoltage();
            int bp = constrain((int)((bv - 3.0f) / (4.2f - 3.0f) * 100), 0, 100);
            int nKeys = 0, nSta = 0, nBuf = 0;
            for (uint8_t i = 0; i < KEY_CACHE_SIZE; i++)
                if (peerKeys[i].valid)
                    nKeys++;
            for (uint8_t i = 0; i < STATION_CACHE_SIZE; i++)
                if (stations[i].node)
                    nSta++;
            for (uint8_t i = 0; i < OUTBOUND_BUFFER_SIZE; i++)
                if (outboundBuffer[i].active)
                    nBuf++;
            pf("{\"nodeId\":\"0x%X\",\"maxHops\":%u,\"weatherMode\":%s,\"personalKeyValid\":%s",
               nodeId, configuredMaxHops,
               weatherModeEnabled ? "true" : "false",
               personalKeyValid ? "true" : "false");
            if (personalKeyValid)
                pf(",\"personalKey\":\"%s\"", keyToHex(personalKey).c_str());
            pf(",\"loraFreq\":%.1f,\"loraBW\":%.1f,\"loraSF\":%u,\"loraCR\":%u,\"loraPower\":%d",
               LORA_FREQUENCY, LORA_BANDWIDTH, LORA_SF, LORA_CR, LORA_POWER);
            pf(",\"bleConnected\":%s,\"peerKeys\":%d,\"stations\":%d",
               bleConnected ? "true" : "false", nKeys, nSta);
            pf(",\"reliableSend\":%s,\"outboundBuffered\":%d",
               reliableSendEnabled ? "true" : "false", nBuf);
            pf(",\"batteryV\":%.2f,\"batteryPct\":%d", bv, bp);
            pf(",\"sleepEnabled\":%s,\"sleepIdleMs\":%lu}\n",
               sleepEnabled ? "true" : "false", sleepIdleMs);
            return;
        }

        if (line == "/id")
        {
            out.print("Node ID: 0x");
            out.println(nodeId, HEX);
            return;
        }

        if (line.startsWith("/ttl "))
        {
            int requested = line.substring(5).toInt();
            if (requested >= 1 && requested <= 15)
            {
                configuredMaxHops = static_cast<uint8_t>(requested);
                out.print("maxHops gesetzt auf ");
                out.println(configuredMaxHops);
            }
            else
            {
                out.println("Ungueltiger Wert. Erlaubt: 1..15");
            }
            return;
        }

        if (line == "/scan")
        {
            sendDiscoveryRequest();
            return;
        }

        if (line == "/stations")
        {
            printStations();
            return;
        }

        if (line == "/wx on")
        {
            weatherModeEnabled = true;
            out.println("Weather-Mode: ON");
            return;
        }

        if (line == "/wx off")
        {
            weatherModeEnabled = false;
            out.println("Weather-Mode: OFF");
            return;
        }

        if (line == "/wx status")
        {
            out.print("Weather-Mode: ");
            out.println(weatherModeEnabled ? "ON" : "OFF");
            return;
        }

        if (line == "/wxreq" || line == "/wxreq all")
        {
            sendWeatherRequest(MESH_BROADCAST);
            return;
        }

        if (line.startsWith("/wxreq "))
        {
            uint16_t targetNode = 0;
            String targetToken = line.substring(7);
            targetToken.trim();
            if (parseNodeValue(targetToken, targetNode))
            {
                sendWeatherRequest(targetNode);
            }
            else
            {
                out.println("Ungueltiges Node-Format. Beispiel: /wxreq 0x12AF");
            }
            return;
        }

        if (line == "/mykey gen")
        {
            generatePersonalKey();
            String k = keyToHex(personalKey);
            pf("Eigener Key neu generiert.\nFuer andere Node: /key set 0x%X %s\n", nodeId, k.c_str());
            return;
        }

        if (line.startsWith("/mykey set "))
        {
            String keyToken = line.substring(11);
            keyToken.trim();
            uint8_t parsedKey[KEY_BYTES];
            if (!parseHexKey(keyToken, parsedKey))
            {
                out.println("Ungueltiger Key. Erwartet 32 Hex-Zeichen.");
                return;
            }
            memcpy(personalKey, parsedKey, KEY_BYTES);
            personalKeyValid = true;
            String k = keyToHex(personalKey);
            pf("Eigener Key gesetzt: %s\nFuer andere Node: /key set 0x%X %s\n", k.c_str(), nodeId, k.c_str());
            return;
        }

        if (line == "/mykey show")
        {
            if (!personalKeyValid)
                out.println("Kein eigener Key gesetzt. Nutze: /mykey gen");
            else
                pf("Eigener Key fuer ID 0x%X: %s\n", nodeId, keyToHex(personalKey).c_str());
            return;
        }

        if (line == "/keys")
        {
            printPeerKeys();
            return;
        }

        if (line.startsWith("/key set "))
        {
            String rest = line.substring(9);
            rest.trim();
            int split = rest.indexOf(' ');
            if (split <= 0)
            {
                out.println("Syntax: /key set <nodeId> <hex32>");
                return;
            }

            String nodeToken = rest.substring(0, split);
            String keyToken = rest.substring(split + 1);
            nodeToken.trim();
            keyToken.trim();

            uint16_t keyNode = 0;
            uint8_t parsedKey[KEY_BYTES];
            if (!parseNodeValue(nodeToken, keyNode))
            {
                out.println("Ungueltige Node-ID.");
                return;
            }
            if (!parseHexKey(keyToken, parsedKey))
            {
                out.println("Ungueltiger Key. Erwartet 32 Hex-Zeichen.");
                return;
            }

            if (!setPeerKey(keyNode, parsedKey))
            {
                out.println("Key-Speicher voll.");
                return;
            }

            out.print("Key gespeichert fuer Node 0x");
            out.println(keyNode, HEX);
            return;
        }

        if (line.startsWith("/key del "))
        {
            uint16_t keyNode = 0;
            String nodeToken = line.substring(9);
            nodeToken.trim();
            if (!parseNodeValue(nodeToken, keyNode))
            {
                out.println("Ungueltige Node-ID.");
                return;
            }

            if (deletePeerKey(keyNode))
            {
                out.print("Key geloescht fuer Node 0x");
                out.println(keyNode, HEX);
            }
            else
            {
                out.println("Kein Key fuer diese Node vorhanden.");
            }
            return;
        }

        if (line.startsWith("/msg "))
        {
            String rest = line.substring(5);
            rest.trim();
            int split = rest.indexOf(' ');
            if (split <= 0)
            {
                out.println("Syntax: /msg <nodeId> <text>");
                return;
            }

            String nodeToken = rest.substring(0, split);
            String textToken = rest.substring(split + 1);
            nodeToken.trim();
            textToken.trim();

            uint16_t targetNode = 0;
            if (!parseNodeValue(nodeToken, targetNode))
            {
                out.println("Ungueltige Node-ID.");
                return;
            }

            sendMsg(targetNode, textToken);
            return;
        }

        if (line.startsWith("/pub "))
        {
            String text = line.substring(5);
            text.trim();
            if (text.length() == 0)
            {
                out.println("Syntax: /pub <text>");
                return;
            }
            sendPublicMessage(text);
            return;
        }

        if (line.startsWith("/eto "))
        {
            String rest = line.substring(5);
            rest.trim();
            int split = rest.indexOf(' ');
            if (split <= 0)
            {
                out.println("Syntax: /eto <nodeId> <text>");
                return;
            }

            String nodeToken = rest.substring(0, split);
            String textToken = rest.substring(split + 1);
            nodeToken.trim();
            textToken.trim();

            uint16_t targetNode = 0;
            if (!parseNodeValue(nodeToken, targetNode))
            {
                out.println("Ungueltige Node-ID.");
                return;
            }

            sendMsg(targetNode, textToken, true);
            return;
        }

        if (line.startsWith("/wxloc "))
        {
            String rest = line.substring(7);
            rest.trim();

            if (rest == "show")
            {
                if (wxLocationSet)
                    pf("WX Location: lat=%.6f lon=%.6f\n", wxLatitude, wxLongitude);
                else
                    out.println("Kein Standort gesetzt. Nutze: /wxloc <lat> <lon>");
                return;
            }

            int split = rest.indexOf(' ');
            if (split <= 0)
            {
                out.println("Syntax: /wxloc <lat> <lon> oder /wxloc show");
                return;
            }

            String latToken = rest.substring(0, split);
            String lonToken = rest.substring(split + 1);
            latToken.trim();
            lonToken.trim();

            wxLatitude = latToken.toFloat();
            wxLongitude = lonToken.toFloat();
            wxLocationSet = true;

            pf("WX Location gesetzt: lat=%.6f lon=%.6f\n", wxLatitude, wxLongitude);
            return;
        }

        // ── Traceroute command ───────────────────────────────────────
        if (line.startsWith("/traceroute "))
        {
            String nodeToken = line.substring(12);
            nodeToken.trim();
            uint16_t targetNode = 0;
            if (!parseNodeValue(nodeToken, targetNode))
            {
                out.println("Ungueltige Node-ID. Beispiel: /traceroute 0x12AF");
                return;
            }
            sendTraceRoute(targetNode);
            return;
        }

        // ── Reliable send toggle ─────────────────────────────────────
        if (line == "/reliable on")
        {
            reliableSendEnabled = true;
            out.println("Reliable send: ON");
            return;
        }

        if (line == "/reliable off")
        {
            reliableSendEnabled = false;
            // Clear outbound buffer when turning off
            for (uint8_t i = 0; i < OUTBOUND_BUFFER_SIZE; i++)
                outboundBuffer[i].active = false;
            out.println("Reliable send: OFF (buffer cleared)");
            return;
        }

        if (line == "/reliable status")
        {
            out.print("Reliable send: ");
            out.println(reliableSendEnabled ? "ON" : "OFF");
            return;
        }

        // ── Show outbound buffer ─────────────────────────────────────
        if (line == "/buffer")
        {
            printOutboundBuffer();
            return;
        }

        // ── Battery command ──────────────────────────────────────────
        if (line == "/battery")
        {
            printBattery();
            return;
        }

        // ── Sleep commands ───────────────────────────────────────────
        if (line == "/sleep")
        {
            enterSleep();
            return; // won't reach here after deep sleep
        }

        if (line == "/autosleep on")
        {
            sleepEnabled = true;
            resetIdleTimer();
            pf("Auto-sleep: ON (idle %lus)\n", sleepIdleMs / 1000UL);
            return;
        }

        if (line == "/autosleep off")
        {
            sleepEnabled = false;
            out.println("Auto-sleep: OFF");
            return;
        }

        if (line == "/autosleep status")
        {
            pf("Auto-sleep: %s (idle %lus)\n", sleepEnabled ? "ON" : "OFF", sleepIdleMs / 1000UL);
            return;
        }

        if (line.startsWith("/autosleep "))
        {
            int secs = line.substring(11).toInt();
            if (secs >= 5 && secs <= 3600)
            {
                sleepIdleMs = (uint32_t)secs * 1000UL;
                pf("Auto-sleep idle set to %ds\n", secs);
            }
            else
                out.println("Ungueltiger Wert. Erlaubt: 5..3600");
            return;
        }

        resetIdleTimer();
        sendMsg(MESH_BROADCAST, line);
}

void readSerialInput()
{
        while (Serial.available())
        {
            char c = static_cast<char>(Serial.read());
            if (c == '\r')
            {
                continue;
            }
            if (c == '\n')
            {
                String line = serialLine;
                serialLine = "";
                handleSerialLine(line);
                continue;
            }

            if (serialLine.length() < MAX_MESH_PAYLOAD)
            {
                serialLine += c;
            }
        }
}

void setup()
{
        Serial.begin(115200);
        delay(400);

        uint64_t mac = ESP.getEfuseMac();
        nodeId = static_cast<uint16_t>(mac & 0xFFFF);
        randomSeed(static_cast<unsigned long>(micros()) ^ static_cast<unsigned long>(nodeId));

        SPI.begin(PIN_SCK, PIN_MISO, PIN_MOSI, PIN_NSS);

        int16_t state = radio.begin(
            LORA_FREQUENCY,
            LORA_BANDWIDTH,
            LORA_SF,
            LORA_CR,
            LORA_SYNC_WORD,
            LORA_POWER,
            LORA_PREAMBLE,
            LORA_TCXO_VOLTAGE,
            false);

        if (state != RADIOLIB_ERR_NONE)
        {
            out.print("LoRa init Fehler: ");
            out.println(state);
            while (true)
            {
                delay(1000);
            }
        }

        radio.setDio1Action(setRadioFlag);

        restartReceive();

        BLEDevice::init("MegaMesh");
        BLEServer *pServer = BLEDevice::createServer();
        pServer->setCallbacks(new MeshBLEServerCB());
        BLEService *pSvc = pServer->createService("6E400001-B5A3-F393-E0A9-E50E24DCCA9E");
        pTxChar = pSvc->createCharacteristic("6E400003-B5A3-F393-E0A9-E50E24DCCA9E", BLECharacteristic::PROPERTY_NOTIFY);
        pTxChar->addDescriptor(new BLE2902());
        BLECharacteristic *pRxChar = pSvc->createCharacteristic("6E400002-B5A3-F393-E0A9-E50E24DCCA9E", BLECharacteristic::PROPERTY_WRITE | BLECharacteristic::PROPERTY_WRITE_NR);
        pRxChar->setCallbacks(new MeshBLERxCB());
        pSvc->start();
        BLEDevice::startAdvertising();

        out.println("Mesh gestartet ");
        pf("Node ID: 0x%X\n", nodeId);
        out.println("Weather-Mode: OFF");
        out.println("Encryption: /mykey gen for eigenen Node-Key");
        out.println("BLE: MegaMesh (NUS)");
        pf("Pins NSS/SCK/MOSI/MISO/RST/BUSY/DIO1: %u/%u/%u/%u/%u/%u/%u\n",
           PIN_NSS, PIN_SCK, PIN_MOSI, PIN_MISO, PIN_RST, PIN_BUSY, PIN_DIO1);
        printBattery();
        lastActivityMs = millis();
        printHelp();
}

void loop()
{
        readSerialInput();

        // Process all queued BLE commands
        while (bleCmdHead != bleCmdTail)
        {
            String cmd = bleCmdQueue[bleCmdTail];
            bleCmdTail = (bleCmdTail + 1) % BLE_CMD_QUEUE_SIZE;
            handleSerialLine(cmd);
        }

        if (radioIrq)
        {
            radioIrq = false;
            resetIdleTimer();
            handleReceivedPacket();
        }

        // Retry buffered messages awaiting ACK
        processOutboundBuffer();

        // Auto-sleep when idle
        if (sleepEnabled && (millis() - lastActivityMs >= sleepIdleMs))
        {
            out.println("Auto-sleep: idle timeout reached.");
            enterSleep();
        }
}
