#include <Arduino.h>
#include <SPI.h>
#include <RadioLib.h>

static const uint8_t PIN_NSS = 8;
static const uint8_t PIN_SCK = 9;
static const uint8_t PIN_MOSI = 10;
static const uint8_t PIN_MISO = 11;
static const uint8_t PIN_RST = 12;
static const uint8_t PIN_BUSY = 13;
static const uint8_t PIN_DIO1 = 14;

static const float LORA_FREQUENCY = 868.0;
static const float LORA_BANDWIDTH = 125.0;
static const uint8_t LORA_SF = 9;
static const uint8_t LORA_CR = 7;
static const uint8_t LORA_SYNC_WORD = 0x12;
static const int8_t LORA_POWER = 17;
static const uint16_t LORA_PREAMBLE = 8;
static const float LORA_TCXO_VOLTAGE = 1.6;

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
static const char *CTRL_DISC_REQ = "#MESH_DISC_REQ";
static const char *CTRL_DISC_RESP = "#MESH_DISC_RESP";
static const char *CTRL_WX_REQ = "#MESH_WX_REQ";
static const char *CTRL_WX_DATA = "#MESH_WX_DATA";

struct SeenEntry
{
    uint16_t origin;
    uint16_t msgId;
    uint32_t seenAt;
};

struct StationEntry
{
    uint16_t node;
    uint32_t lastSeen;
    float rssi;
    float snr;
    uint8_t hops;
};

struct PeerKeyEntry
{
    bool valid;
    uint16_t node;
    uint8_t key[KEY_BYTES];
};

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

Module radioModule(PIN_NSS, PIN_DIO1, PIN_RST, PIN_BUSY);
SX1262 radio(&radioModule);

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
{
    Serial.println("Gespeicherte Node-Keys:");
    bool any = false;
    for (uint8_t i = 0; i < KEY_CACHE_SIZE; i++)
    {
        if (!peerKeys[i].valid)
        {
            continue;
        }
        any = true;
        Serial.print("- node=0x");
        Serial.print(peerKeys[i].node, HEX);
        Serial.print(" key=");
        Serial.println(keyToHex(peerKeys[i].key));
    }
    if (!any)
    {
        Serial.println("(keine)");
    }
}

void cryptPayload(uint8_t *buffer, size_t len, const uint8_t *key, const MeshHeader &header)
{
    uint32_t state = 2166136261UL;
    state ^= header.origin;
    state *= 16777619UL;
    state ^= (static_cast<uint32_t>(header.destination) << 16) | header.msgId;
    state *= 16777619UL;

    for (uint8_t i = 0; i < KEY_BYTES; i++)
    {
        state ^= key[i];
        state *= 16777619UL;
    }

    for (size_t i = 0; i < len; i++)
    {
        state ^= static_cast<uint32_t>(i + 1);
        state = state * 1664525UL + 1013904223UL;
        uint8_t ks = static_cast<uint8_t>(state >> 24);
        buffer[i] ^= ks;
    }
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
    Serial.println("Gefundene Stationen:");
    bool any = false;
    uint32_t nowMs = millis();
    for (uint8_t index = 0; index < STATION_CACHE_SIZE; index++)
    {
        if (stations[index].node == 0)
        {
            continue;
        }
        any = true;
        Serial.print("- 0x");
        Serial.print(stations[index].node, HEX);
        Serial.print(" last=");
        Serial.print((nowMs - stations[index].lastSeen) / 1000UL);
        Serial.print("s rssi=");
        Serial.print(stations[index].rssi);
        Serial.print(" snr=");
        Serial.print(stations[index].snr);
        Serial.print(" hops=");
        Serial.println(stations[index].hops);
    }

    if (!any)
    {
        Serial.println("(keine)");
    }
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
        Serial.print("RX start error: ");
        Serial.println(state);
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
        Serial.print("TX error: ");
        Serial.println(txState);
        restartReceive();
        return false;
    }

    restartReceive();
    return true;
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
            Serial.println("Kein eigener Key. Nutze: /mykey gen");
            return false;
        }
        cryptPayload(payloadBytes, header.payloadLen, personalKey, header);
    }

    rememberSeen(header.origin, header.msgId);
    return sendMeshFrame(header, payloadBytes);
}

void sendDiscoveryRequest()
{
    if (sendTextTo(MESH_BROADCAST, String(CTRL_DISC_REQ)))
    {
        Serial.println("SCAN gesendet: warte auf Antworten...");
    }
}

void sendDiscoveryResponse(uint16_t destination)
{
    String payload = String(CTRL_DISC_RESP) + ":" + String(nodeId, HEX);
    sendTextTo(destination, payload);
}

String buildWeatherPlaceholder()
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
    payload += ",placeholder=1";
    return payload;
}

void sendWeatherRequest(uint16_t destination)
{
    if (sendTextTo(destination, String(CTRL_WX_REQ)))
    {
        Serial.print("WX request gesendet an ");
        if (destination == MESH_BROADCAST)
        {
            Serial.println("broadcast");
        }
        else
        {
            Serial.print("0x");
            Serial.println(destination, HEX);
        }
    }
}

void sendWeatherResponse(uint16_t destination)
{
    String payload = buildWeatherPlaceholder();
    if (sendTextTo(destination, payload))
    {
        Serial.print("WX response gesendet an 0x");
        Serial.println(destination, HEX);
    }
}

void sendUserMessage(const String &message)
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

    if (sendTextTo(MESH_BROADCAST, payloadText))
    {
        uint16_t sentMsgId = static_cast<uint16_t>(nextMsgId - 1);
        Serial.print("TX msgId=");
        Serial.print(sentMsgId);
        Serial.print(" hops=");
        Serial.print(0);
        Serial.print("/");
        Serial.print(configuredMaxHops);
        Serial.print(" text=");
        Serial.println(payloadText);
    }
}

void sendEncryptedDirectMessage(uint16_t destination, const String &message)
{
    if (destination == MESH_BROADCAST)
    {
        Serial.println("Verschluesselt nur als Direktnachricht erlaubt.");
        return;
    }

    String payloadText = message;
    if (payloadText.length() > MAX_MESH_PAYLOAD)
    {
        payloadText = payloadText.substring(0, MAX_MESH_PAYLOAD);
    }

    if (sendTextTo(destination, payloadText, true))
    {
        uint16_t sentMsgId = static_cast<uint16_t>(nextMsgId - 1);
        Serial.print("ETX to=0x");
        Serial.print(destination, HEX);
        Serial.print(" msgId=");
        Serial.print(sentMsgId);
        Serial.print(" text=");
        Serial.println(payloadText);
    }
}

void relayIfNeeded(const MeshHeader &incoming, const uint8_t *payload, bool alreadySeen)
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

    if (sendMeshFrame(relay, payload))
    {
        Serial.print("RELAY origin=");
        Serial.print(relay.origin, HEX);
        Serial.print(" msgId=");
        Serial.print(relay.msgId);
        Serial.print(" hops=");
        Serial.print(relay.hopCount);
        Serial.print("/");
        Serial.println(relay.maxHops);
    }
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
        Serial.print("RX read error: ");
        Serial.println(state);
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

    if (header.destination == MESH_BROADCAST || header.destination == nodeId)
    {
        bool encrypted = (header.flags & MESH_FLAG_ENCRYPTED) != 0;
        bool decryptedOk = true;
        uint8_t payloadWork[MAX_MESH_PAYLOAD];
        const size_t copyLen = payloadLen <= MAX_MESH_PAYLOAD ? payloadLen : MAX_MESH_PAYLOAD;

        memcpy(payloadWork, payloadPtr, copyLen);
        if (encrypted)
        {
            int keyIndex = findPeerKeyIndex(header.origin);
            if (keyIndex < 0)
            {
                decryptedOk = false;
            }
            else
            {
                cryptPayload(payloadWork, copyLen, peerKeys[keyIndex].key, header);
            }
        }

        char textBuffer[MAX_MESH_PAYLOAD + 1];
        memcpy(textBuffer, payloadWork, copyLen);
        textBuffer[copyLen] = '\0';

        Serial.print("RX origin=");
        Serial.print(header.origin, HEX);
        Serial.print(" msgId=");
        Serial.print(header.msgId);
        Serial.print(" hops=");
        Serial.print(header.hopCount);
        Serial.print("/");
        Serial.print(header.maxHops);
        Serial.print(" rssi=");
        Serial.print(rssi);
        Serial.print(" snr=");
        Serial.print(snr);
        Serial.print(" enc=");
        Serial.print(encrypted ? 1 : 0);
        Serial.print(" text=");
        if (encrypted && !decryptedOk)
        {
            Serial.println("<encrypted: key fehlt fuer origin>");
        }
        else
        {
            Serial.println(textBuffer);
        }

        String payloadText = decryptedOk ? String(textBuffer) : String("");
        if (!alreadySeen && decryptedOk && payloadText == CTRL_DISC_REQ && header.origin != nodeId)
        {
            sendDiscoveryResponse(header.origin);
        }
        else if (decryptedOk && payloadText.startsWith(String(CTRL_DISC_RESP)) && header.origin != nodeId)
        {
            Serial.print("DISCOVERED station=0x");
            Serial.print(header.origin, HEX);
            Serial.print(" hops=");
            Serial.print(header.hopCount);
            Serial.print(" rssi=");
            Serial.print(rssi);
            Serial.print(" snr=");
            Serial.println(snr);
        }

        if (!alreadySeen && decryptedOk && payloadText == CTRL_WX_REQ && header.origin != nodeId && weatherModeEnabled)
        {
            sendWeatherResponse(header.origin);
        }
        else if (decryptedOk && payloadText.startsWith(String(CTRL_WX_DATA)) && header.origin != nodeId)
        {
            Serial.print("WEATHER from=0x");
            Serial.print(header.origin, HEX);
            Serial.print(" hops=");
            Serial.print(header.hopCount);
            Serial.print(" data=");
            Serial.println(payloadText);
        }
    }

    relayIfNeeded(header, payloadPtr, alreadySeen);
}

void printHelp()
{
    Serial.println("Mesh Serial Commands:");
    Serial.println("/help              -> Hilfe anzeigen");
    Serial.println("/id                -> eigene Node-ID anzeigen");
    Serial.println("/ttl <1..15>       -> maxHops setzen");
    Serial.println("/scan              -> Stationen suchen");
    Serial.println("/stations          -> gefundene Stationen anzeigen");
    Serial.println("/wx on|off|status  -> Weather-Mode steuern");
    Serial.println("/wxreq [all|node]  -> Wetterdaten anfragen");
    Serial.println("/mykey gen|show    -> eigener Key fuer eigene Node-ID");
    Serial.println("/key set <id> <hex32> -> Key fuer fremde Node-ID speichern");
    Serial.println("/key del <id>      -> Key fuer Node-ID loeschen");
    Serial.println("/keys              -> gespeicherte Keys anzeigen");
    Serial.println("/eto <id> <text>   -> verschluesselte Direktnachricht");
    Serial.println("jede andere Zeile  -> als Mesh-Nachricht senden");
}

void handleSerialLine(String line)
{
    line.trim();
    if (line.length() == 0)
    {
        return;
    }

    if (line == "/help")
    {
        printHelp();
        return;
    }

    if (line == "/id")
    {
        Serial.print("Node ID: 0x");
        Serial.println(nodeId, HEX);
        return;
    }

    if (line.startsWith("/ttl "))
    {
        int requested = line.substring(5).toInt();
        if (requested >= 1 && requested <= 15)
        {
            configuredMaxHops = static_cast<uint8_t>(requested);
            Serial.print("maxHops gesetzt auf ");
            Serial.println(configuredMaxHops);
        }
        else
        {
            Serial.println("Ungueltiger Wert. Erlaubt: 1..15");
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
        Serial.println("Weather-Mode: ON");
        return;
    }

    if (line == "/wx off")
    {
        weatherModeEnabled = false;
        Serial.println("Weather-Mode: OFF");
        return;
    }

    if (line == "/wx status")
    {
        Serial.print("Weather-Mode: ");
        Serial.println(weatherModeEnabled ? "ON" : "OFF");
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
            Serial.println("Ungueltiges Node-Format. Beispiel: /wxreq 0x12AF");
        }
        return;
    }

    if (line == "/mykey gen")
    {
        generatePersonalKey();
        Serial.println("Eigener Key neu generiert.");
        Serial.print("Fuer andere Node fuer ID 0x");
        Serial.print(nodeId, HEX);
        Serial.print(" setzen mit: /key set 0x");
        Serial.print(nodeId, HEX);
        Serial.print(" ");
        Serial.println(keyToHex(personalKey));
        return;
    }

    if (line == "/mykey show")
    {
        if (!personalKeyValid)
        {
            Serial.println("Kein eigener Key gesetzt. Nutze: /mykey gen");
        }
        else
        {
            Serial.print("Eigener Key fuer ID 0x");
            Serial.print(nodeId, HEX);
            Serial.print(": ");
            Serial.println(keyToHex(personalKey));
        }
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
            Serial.println("Syntax: /key set <nodeId> <hex32>");
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
            Serial.println("Ungueltige Node-ID.");
            return;
        }
        if (!parseHexKey(keyToken, parsedKey))
        {
            Serial.println("Ungueltiger Key. Erwartet 32 Hex-Zeichen.");
            return;
        }

        if (!setPeerKey(keyNode, parsedKey))
        {
            Serial.println("Key-Speicher voll.");
            return;
        }

        Serial.print("Key gespeichert fuer Node 0x");
        Serial.println(keyNode, HEX);
        return;
    }

    if (line.startsWith("/key del "))
    {
        uint16_t keyNode = 0;
        String nodeToken = line.substring(9);
        nodeToken.trim();
        if (!parseNodeValue(nodeToken, keyNode))
        {
            Serial.println("Ungueltige Node-ID.");
            return;
        }

        if (deletePeerKey(keyNode))
        {
            Serial.print("Key geloescht fuer Node 0x");
            Serial.println(keyNode, HEX);
        }
        else
        {
            Serial.println("Kein Key fuer diese Node vorhanden.");
        }
        return;
    }

    if (line.startsWith("/eto "))
    {
        String rest = line.substring(5);
        rest.trim();
        int split = rest.indexOf(' ');
        if (split <= 0)
        {
            Serial.println("Syntax: /eto <nodeId> <text>");
            return;
        }

        String nodeToken = rest.substring(0, split);
        String textToken = rest.substring(split + 1);
        nodeToken.trim();
        textToken.trim();

        uint16_t targetNode = 0;
        if (!parseNodeValue(nodeToken, targetNode))
        {
            Serial.println("Ungueltige Node-ID.");
            return;
        }

        sendEncryptedDirectMessage(targetNode, textToken);
        return;
    }

    sendUserMessage(line);
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
        Serial.print("LoRa init Fehler: ");
        Serial.println(state);
        while (true)
        {
            delay(1000);
        }
    }

    radio.setDio1Action(setRadioFlag);

    restartReceive();

    Serial.println("Mesh gestartet ");
    Serial.print("Node ID: 0x");
    Serial.println(nodeId, HEX);
    Serial.println("Weather-Mode: OFF");
    Serial.println("Encryption: /mykey gen fuer eigenen Node-Key");
    Serial.print("Pins NSS/SCK/MOSI/MISO/RST/BUSY/DIO1: ");
    Serial.print(PIN_NSS);
    Serial.print("/");
    Serial.print(PIN_SCK);
    Serial.print("/");
    Serial.print(PIN_MOSI);
    Serial.print("/");
    Serial.print(PIN_MISO);
    Serial.print("/");
    Serial.print(PIN_RST);
    Serial.print("/");
    Serial.print(PIN_BUSY);
    Serial.print("/");
    Serial.println(PIN_DIO1);
    printHelp();
}

void loop()
{
    readSerialInput();

    if (radioIrq)
    {
        radioIrq = false;
        handleReceivedPacket();
    }
}
