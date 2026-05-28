// MegaMesh Generic – ESP32 + SX1262 bare relay node
// Strips device-specific features (OLED, BLE, battery, PA, sleep, images).
// Fully relays all mesh traffic including image frames from device-specific nodes.

#include <mbedtls/aes.h>
#include <Arduino.h>
#include <SPI.h>
#include <RadioLib.h>
#include <Preferences.h>

struct MeshHeader;
struct Settings;

// ── Pin config (adjust for board) ──
static const uint8_t PIN_NSS = 8;
static const uint8_t PIN_SCK = 9;
static const uint8_t PIN_MOSI = 10;
static const uint8_t PIN_MISO = 11;
static const uint8_t PIN_RST = 12;
static const uint8_t PIN_BUSY = 13;
static const uint8_t PIN_DIO1 = 14;

// ─ LoRa params (megamesh default) ──
static const float FREQ = 869.4;
static const float BW = 125.0;
static const uint8_t SF = 9;
static const uint8_t CR = 5;
static const uint8_t SYNC = 0x12;
static const int8_t MAX_PWR = 22;
static int8_t txPwr = MAX_PWR;
static const uint16_t PREAMBLE = 8;
static const float TCXO_V = 1.6;

// ── Mesh config ──
static const uint16_t MAGIC = 0x4D48;
static const uint8_t VER = 1;
static const uint16_t BROADCAST = 0xFFFF;
static const size_t MAX_PL = 180;
static const size_t MAX_PKT = 220;
static const uint8_t DEF_HOPS = 7;
static const uint8_t SEEN_SZ = 64;
static const uint8_t STA_SZ = 32;
static const uint8_t KEY_SZ = 24;
static const uint8_t KEY_LEN = 16;
static const uint8_t FLAG_ENC = 0x01;

static const uint8_t PUB_KEY[KEY_LEN] = {
    0x4D, 0x45, 0x47, 0x41, 0x4D, 0x45, 0x53, 0x48,
    0x50, 0x55, 0x42, 0x4C, 0x49, 0x43, 0x30, 0x31};

static const char *C_DISC_REQ = "#MESH_DISC_REQ";
static const char *C_DISC_RESP = "#MESH_DISC_RESP";
static const char *C_WX_REQ = "#MESH_WX_REQ";
static const char *C_WX_DATA = "#MESH_WX_DATA";
static const char *C_ACK = "#MESH_ACK";
static const char *C_TRACE_REQ = "#MESH_TRACE_REQ";
static const char *C_TRACE_RESP = "#MESH_TRACE_RESP";

// ── Reliable send config ──
static const uint8_t OUTBUF_SZ = 8;
static const uint8_t MAX_RETRY = 10;
static const uint32_t RETRY_MS = 5000;
static const uint32_t OUTBOUND_TICK = 25;

// ── LBT config ──
static const float LBT_THRESH = -90.0f;
static const uint8_t LBT_TRIES = 3;
static const uint32_t LBT_DELAY = 150;

// ── Image relay buffer (relay only, no originate) ──
#define IMG_BUF_SZ 4

// ── Data structures ──
struct SeenEntry
{
    uint16_t origin;
    uint16_t msgId;
    uint32_t at;
};
struct StaEntry
{
    uint16_t node;
    uint32_t seen;
    float rssi;
    float snr;
    uint8_t hops;
};
struct PeerKey
{
    bool valid;
    uint16_t node;
    uint8_t key[KEY_LEN];
};

#pragma pack(push, 1)
struct MeshHeader
{
    uint16_t magic;
    uint8_t version;
    uint16_t origin;
    uint16_t msgId;
    uint16_t dest;
    uint8_t hops;
    uint8_t maxHops;
    uint8_t flags;
    uint8_t plLen;
};
#pragma pack(pop)

struct OutEntry
{
    bool active;
    MeshHeader hdr;
    uint8_t pl[MAX_PL];
    uint8_t retries;
    uint32_t lastSent;
};

struct ImgRelay
{
    bool active;
    MeshHeader hdr;
    uint8_t pl[MAX_PL];
    uint8_t retries;
    uint32_t nextAt;
};

// ── Radio ──
Module mod(PIN_NSS, PIN_DIO1, PIN_RST, PIN_BUSY);
SX1262 radio(&mod);

// ── State ──
volatile bool rxFlag = false;
uint16_t myId = 0;
uint16_t msgSeq = 1;
uint8_t maxHops = DEF_HOPS;
bool wxMode = false;
bool reliable = true;
String serBuf;

SeenEntry seen[SEEN_SZ];
uint8_t seenPos = 0;
StaEntry sta[STA_SZ];
PeerKey peers[KEY_SZ];
bool myKeyValid = false;
uint8_t myKey[KEY_LEN] = {0};
OutEntry outbuf[OUTBUF_SZ];
ImgRelay imgBuf[IMG_BUF_SZ];

// ── NVS ──
static const uint32_t CFG_MAGIC = 0x4D534859;
static const uint32_t SAVE_DELAY = 3000;

#pragma pack(push, 1)
struct Settings
{
    uint32_t magic;
    uint16_t nodeId;
    uint8_t maxHops;
    int8_t txPower;
    bool wxMode;
    bool reliable;
    bool myKeyValid;
    uint8_t myKey[16];
    uint8_t peerCount;
    struct
    {
        bool valid;
        uint16_t node;
        uint8_t key[16];
    } peers[24];
};
#pragma pack(pop)

Preferences nvs;
bool dirty = false;
uint32_t dirtyAt = 0;

// ── Helpers ──

void markDirty()
{
    if (!dirty)
    {
        dirty = true;
        dirtyAt = millis();
    }
}

void buildBlob(Settings &s)
{
    memset(&s, 0, sizeof(s));
    s.magic = CFG_MAGIC;
    s.nodeId = myId;
    s.maxHops = maxHops;
    s.txPower = txPwr;
    s.wxMode = wxMode;
    s.reliable = reliable;
    s.myKeyValid = myKeyValid;
    memcpy(s.myKey, myKey, KEY_LEN);
    s.peerCount = 0;
    for (uint8_t i = 0; i < KEY_SZ; i++)
    {
        s.peers[i].valid = peers[i].valid;
        s.peers[i].node = peers[i].node;
        memcpy(s.peers[i].key, peers[i].key, KEY_LEN);
        if (peers[i].valid)
            s.peerCount++;
    }
}

void saveFlash()
{
    Settings s;
    buildBlob(s);
    nvs.begin("meshcfg", true);
    size_t len = nvs.getBytesLength("settings");
    bool same = false;
    if (len == sizeof(s))
    {
        uint8_t *old = (uint8_t *)malloc(sizeof(s));
        if (old)
        {
            nvs.getBytes("settings", old, sizeof(s));
            same = (memcmp(old, &s, sizeof(s)) == 0);
            free(old);
        }
    }
    nvs.end();
    if (same)
    {
        dirty = false;
        return;
    }
    nvs.begin("meshcfg", false);
    nvs.putBytes("settings", &s, sizeof(s));
    nvs.end();
    dirty = false;
    Serial.println("[NVS] saved");
}

bool loadFlash()
{
    nvs.begin("meshcfg", true);
    size_t len = nvs.getBytesLength("settings");
    if (len != sizeof(Settings))
    {
        nvs.end();
        return false;
    }
    Settings s;
    nvs.getBytes("settings", &s, sizeof(s));
    nvs.end();
    if (s.magic != CFG_MAGIC)
        return false;
    myId = s.nodeId;
    maxHops = s.maxHops;
    txPwr = s.txPower;
    if (txPwr < 2)
        txPwr = 2;
    if (txPwr > MAX_PWR)
        txPwr = MAX_PWR;
    wxMode = s.wxMode;
    reliable = s.reliable;
    myKeyValid = s.myKeyValid;
    memcpy(myKey, s.myKey, KEY_LEN);
    for (uint8_t i = 0; i < KEY_SZ; i++)
    {
        peers[i].valid = s.peers[i].valid;
        peers[i].node = s.peers[i].node;
        memcpy(peers[i].key, s.peers[i].key, KEY_LEN);
    }
    return true;
}

void saveIfNeeded()
{
    if (!dirty)
        return;
    if (millis() - dirtyAt < SAVE_DELAY)
        return;
    saveFlash();
}

// ── Crypto ──

void crypt(uint8_t *buf, size_t len, const uint8_t *key, const MeshHeader &h)
{
    mbedtls_aes_context aes;
    mbedtls_aes_init(&aes);
    mbedtls_aes_setkey_enc(&aes, key, 128);
    uint8_t iv[16] = {0};
    iv[0] = (h.origin >> 8) & 0xFF;
    iv[1] = h.origin & 0xFF;
    iv[2] = (h.dest >> 8) & 0xFF;
    iv[3] = h.dest & 0xFF;
    iv[4] = (h.msgId >> 8) & 0xFF;
    iv[5] = h.msgId & 0xFF;
    size_t off = 0;
    uint8_t sb[16] = {0};
    mbedtls_aes_crypt_ctr(&aes, len, &off, iv, sb, buf, buf);
    mbedtls_aes_free(&aes);
}

// ── Station tracking ──

int findSta(uint16_t node)
{
    for (uint8_t i = 0; i < STA_SZ; i++)
        if (sta[i].node == node)
            return i;
    return -1;
}

void updateSta(uint16_t node, float rssi, float snr, uint8_t hops)
{
    int s = findSta(node);
    if (s >= 0)
    {
        sta[s].seen = millis();
        if (hops <= sta[s].hops)
        {
            sta[s].rssi = rssi;
            sta[s].snr = snr;
            sta[s].hops = hops;
        }
        return;
    }
    for (uint8_t i = 0; i < STA_SZ; i++)
    {
        if (sta[i].node == 0)
        {
            s = i;
            break;
        }
    }
    if (s < 0)
    {
        uint8_t worst = 0;
        for (uint8_t i = 1; i < STA_SZ; i++)
        {
            if (sta[i].hops > sta[worst].hops ||
                (sta[i].hops == sta[worst].hops && sta[i].seen < sta[worst].seen))
                worst = i;
        }
        s = worst;
    }
    sta[s] = {node, millis(), rssi, snr, hops};
}

void printSta()
{
    Serial.println("Stations:");
    bool any = false;
    uint32_t now = millis();
    for (uint8_t i = 0; i < STA_SZ; i++)
    {
        if (!sta[i].node)
            continue;
        any = true;
        Serial.printf("- 0x%04X  %lus  rssi=%.0f snr=%.1f hops=%d\n",
                      sta[i].node, (now - sta[i].seen) / 1000UL, sta[i].rssi, sta[i].snr, sta[i].hops);
    }
    if (!any)
        Serial.println("(none)");
}

// ── Seen cache ──

bool wasSeen(uint16_t origin, uint16_t id)
{
    uint32_t now = millis();
    for (uint8_t i = 0; i < SEEN_SZ; i++)
    {
        if (seen[i].origin == origin && seen[i].msgId == id && (now - seen[i].at) < 120000)
            return true;
    }
    return false;
}

void markSeen(uint16_t origin, uint16_t id)
{
    seen[seenPos] = {origin, id, millis()};
    seenPos = (seenPos + 1) % SEEN_SZ;
}

// ── Key management ──

int hexNib(char c)
{
    if (c >= '0' && c <= '9')
        return c - '0';
    if (c >= 'a' && c <= 'f')
        return 10 + c - 'a';
    if (c >= 'A' && c <= 'F')
        return 10 + c - 'A';
    return -1;
}

String keyHex(const uint8_t *k)
{
    const char *d = "0123456789ABCDEF";
    String o;
    o.reserve(KEY_LEN * 2);
    for (uint8_t i = 0; i < KEY_LEN; i++)
    {
        o += d[(k[i] >> 4) & 0x0F];
        o += d[k[i] & 0x0F];
    }
    return o;
}

bool parseHex(String raw, uint8_t *out)
{
    raw.trim();
    if (raw.startsWith("0x") || raw.startsWith("0X"))
        raw = raw.substring(2);
    if (raw.length() != KEY_LEN * 2)
        return false;
    for (uint8_t i = 0; i < KEY_LEN; i++)
    {
        int hi = hexNib(raw[i * 2]), lo = hexNib(raw[i * 2 + 1]);
        if (hi < 0 || lo < 0)
            return false;
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return true;
}

bool parseNode(const String &s, uint16_t &out)
{
    String r = s;
    r.trim();
    if (!r.length())
        return false;
    char *ep = nullptr;
    unsigned long v = strtoul(r.c_str(), &ep, 0);
    if (ep == r.c_str() || *ep != '\0' || v > 0xFFFF)
        return false;
    out = (uint16_t)v;
    return true;
}

void genKey()
{
    for (uint8_t i = 0; i < KEY_LEN; i++)
        myKey[i] = (uint8_t)random(0, 256);
    myKeyValid = true;
}

int findPeer(uint16_t node)
{
    for (uint8_t i = 0; i < KEY_SZ; i++)
        if (peers[i].valid && peers[i].node == node)
            return i;
    return -1;
}

bool setPeer(uint16_t node, const uint8_t *key)
{
    int idx = findPeer(node);
    if (idx < 0)
    {
        for (uint8_t i = 0; i < KEY_SZ; i++)
            if (!peers[i].valid)
            {
                idx = i;
                break;
            }
    }
    if (idx < 0)
        return false;
    peers[idx] = {true, node, {}};
    memcpy(peers[idx].key, key, KEY_LEN);
    return true;
}

bool delPeer(uint16_t node)
{
    int idx = findPeer(node);
    if (idx < 0)
        return false;
    peers[idx].valid = false;
    peers[idx].node = 0;
    memset(peers[idx].key, 0, KEY_LEN);
    return true;
}

void printKeys()
{
    Serial.println("Peer keys:");
    bool any = false;
    for (uint8_t i = 0; i < KEY_SZ; i++)
    {
        if (!peers[i].valid)
            continue;
        any = true;
        Serial.printf("- 0x%04X  %s\n", peers[i].node, keyHex(peers[i].key).c_str());
    }
    if (!any)
        Serial.println("(none)");
}

// ── Radio helpers ──

#if defined(ESP8266) || defined(ESP32)
ICACHE_RAM_ATTR
#endif
void onRx(void) { rxFlag = true; }

void startRx()
{
    rxFlag = false;
    radio.startReceive();
}

bool sendFrame(MeshHeader &h, const uint8_t *pl)
{
    size_t len = sizeof(MeshHeader) + h.plLen;
    if (len > MAX_PKT)
        return false;
    uint8_t frame[MAX_PKT];
    memcpy(frame, &h, sizeof(MeshHeader));
    if (h.plLen > 0)
        memcpy(frame + sizeof(MeshHeader), pl, h.plLen);
    int16_t st = radio.transmit(frame, len);
    if (st != RADIOLIB_ERR_NONE)
    {
        Serial.printf("TX err: %d\n", st);
        startRx();
        return false;
    }
    startRx();
    return true;
}

bool channelFree() { return radio.getRSSI() < LBT_THRESH; }

bool sendLBT(MeshHeader &h, const uint8_t *pl)
{
    for (uint8_t i = 0; i < LBT_TRIES; i++)
    {
        if (channelFree())
            return sendFrame(h, pl);
        delay(LBT_DELAY + random(0, 50));
    }
    Serial.println("LBT: busy, dropped");
    return false;
}

// ── Image relay buffer ──

bool bufImg(const MeshHeader &h, const uint8_t *pl)
{
    for (uint8_t i = 0; i < IMG_BUF_SZ; i++)
    {
        if (!imgBuf[i].active)
        {
            imgBuf[i] = {true, h, {}, 0, millis() + random(20, 80)};
            memcpy(imgBuf[i].pl, pl, h.plLen);
            return true;
        }
    }
    return false;
}

void processImg()
{
    uint32_t now = millis();
    for (uint8_t i = 0; i < IMG_BUF_SZ; i++)
    {
        if (!imgBuf[i].active || now < imgBuf[i].nextAt)
            continue;
        if (channelFree())
        {
            sendFrame(imgBuf[i].hdr, imgBuf[i].pl);
            imgBuf[i].active = false;
        }
        else
        {
            imgBuf[i].retries++;
            if (imgBuf[i].retries >= LBT_TRIES)
                imgBuf[i].active = false;
            else
                imgBuf[i].nextAt = now + LBT_DELAY + random(0, 100);
        }
    }
}

// ── Outbound reliable buffer ──

bool bufOut(const MeshHeader &h, const uint8_t *pl)
{
    for (uint8_t i = 0; i < OUTBUF_SZ; i++)
    {
        if (!outbuf[i].active)
        {
            outbuf[i] = {true, h, {}, 0, millis()};
            memcpy(outbuf[i].pl, pl, h.plLen);
            return true;
        }
    }
    return false;
}

void removeOut(uint16_t origin, uint16_t id)
{
    for (uint8_t i = 0; i < OUTBUF_SZ; i++)
    {
        if (outbuf[i].active && outbuf[i].hdr.origin == origin && outbuf[i].hdr.msgId == id)
        {
            outbuf[i].active = false;
            Serial.printf("ACK msgId=%d removed\n", id);
            return;
        }
    }
}

void processOut()
{
    if (!reliable)
        return;
    uint32_t now = millis();
    for (uint8_t i = 0; i < OUTBUF_SZ; i++)
    {
        if (!outbuf[i].active)
            continue;
        if (now - outbuf[i].lastSent < RETRY_MS)
            continue;
        outbuf[i].retries++;
        if (outbuf[i].retries > MAX_RETRY)
        {
            Serial.printf("FAIL msgId=%d to=0x%04X dropped\n", outbuf[i].hdr.msgId, outbuf[i].hdr.dest);
            outbuf[i].active = false;
            continue;
        }
        Serial.printf("RETRY #%d msgId=%d to=0x%04X\n", outbuf[i].retries, outbuf[i].hdr.msgId, outbuf[i].hdr.dest);
        sendFrame(outbuf[i].hdr, outbuf[i].pl);
        outbuf[i].lastSent = now;
    }
}

// ── Send helpers ──

bool sendTo(uint16_t dest, const String &text, bool enc = false, bool lbt = false)
{
    if (text.length() == 0)
        return false;
    String pl = text.substring(0, MAX_PL);
    MeshHeader h;
    h.magic = MAGIC;
    h.version = VER;
    h.origin = myId;
    h.msgId = msgSeq++;
    h.dest = dest;
    h.hops = 0;
    h.maxHops = maxHops;
    h.flags = enc ? FLAG_ENC : 0;
    h.plLen = (uint8_t)pl.length();

    uint8_t buf[MAX_PL];
    memcpy(buf, pl.c_str(), h.plLen);
    if (enc)
    {
        if (!myKeyValid)
        {
            Serial.println("No key. Use /mykey gen");
            return false;
        }
        crypt(buf, h.plLen, myKey, h);
    }
    markSeen(h.origin, h.msgId);
    bool ok = lbt ? sendLBT(h, buf) : sendFrame(h, buf);
    if (ok && reliable && dest != BROADCAST && !pl.startsWith("#MESH_"))
        bufOut(h, buf);
    return ok;
}

void sendAck(uint16_t dest, uint16_t aOrg, uint16_t aMid)
{
    sendTo(dest, String(C_ACK) + ":" + String(aOrg, HEX) + ":" + String(aMid));
}

void sendDiscReq() { sendTo(BROADCAST, String(C_DISC_REQ)); }

void sendDiscResp(uint16_t req)
{
    sendTo(BROADCAST, String(C_DISC_RESP) + ":0x" + String(myId, HEX) + ":0x" + String(req, HEX));
}

void sendWxResp(uint16_t dest)
{
    if (!wxMode)
        return;
    String p = String(C_WX_DATA) + ":node=0x" + String(myId, HEX) + ",tempC=0.0,hum=0.0,hPa=0.0";
    sendTo(dest, p);
}

void sendTrace(uint16_t dest)
{
    if (dest == BROADCAST)
    {
        Serial.println("Traceroute needs a node ID");
        return;
    }
    sendTo(dest, String(C_TRACE_REQ) + ":0x" + String(myId, HEX));
}

// ── Relay ──

void relay(const MeshHeader &in, const uint8_t *pl, bool seen, const char *txt = nullptr)
{
    if (seen || in.hops >= in.maxHops)
        return;
    MeshHeader r = in;
    r.hops = in.hops + 1;
    markSeen(r.origin, r.msgId);

    if (txt)
    {
        String t(txt);
        if (t.startsWith(String(C_TRACE_REQ)))
        {
            String np = t + ">0x" + String(myId, HEX);
            if (np.length() <= MAX_PL)
            {
                r.plLen = (uint8_t)np.length();
                uint8_t mp[MAX_PL];
                memcpy(mp, np.c_str(), r.plLen);
                sendFrame(r, mp);
                Serial.printf("RELAY TRACE origin=0x%04X hops=%d/%d\n", r.origin, r.hops, r.maxHops);
                return;
            }
        }
    }
    if (sendFrame(r, pl))
        Serial.printf("RELAY origin=0x%04X msgId=%d hops=%d/%d\n", r.origin, r.msgId, r.hops, r.maxHops);
}

void relayImg(const MeshHeader &in, const uint8_t *pl, bool seen)
{
    if (seen || in.dest == myId || in.hops >= in.maxHops)
        return;
    MeshHeader r = in;
    r.hops = in.hops + 1;
    markSeen(r.origin, r.msgId);
    if (bufImg(r, pl))
        Serial.printf("IMG_RELAY buffered msgId=%d\n", r.msgId);
    else
        Serial.println("IMG_RELAY full, dropped");
}

// ── Receive handler ──

void handleRx()
{
    size_t pktLen = radio.getPacketLength();
    if (pktLen < sizeof(MeshHeader) || pktLen > MAX_PKT)
    {
        startRx();
        return;
    }

    uint8_t frame[MAX_PKT];
    if (radio.readData(frame, pktLen) != RADIOLIB_ERR_NONE)
    {
        startRx();
        return;
    }

    MeshHeader h;
    memcpy(&h, frame, sizeof(MeshHeader));
    if (h.magic != MAGIC || h.version != VER || h.origin == myId)
    {
        startRx();
        return;
    }

    size_t plLen = h.plLen < (pktLen - sizeof(MeshHeader)) ? h.plLen : (pktLen - sizeof(MeshHeader));
    const uint8_t *plPtr = frame + sizeof(MeshHeader);
    bool alreadySeen = wasSeen(h.origin, h.msgId);
    float rssi = radio.getRSSI(), snr = radio.getSNR();

    if (!alreadySeen)
        markSeen(h.origin, h.msgId);
    if (h.origin != myId)
        updateSta(h.origin, rssi, snr, h.hops);

    bool decOk = true;
    char txt[MAX_PL + 1];
    txt[0] = '\0';
    size_t cpLen = plLen <= MAX_PL ? plLen : MAX_PL;

    if (h.dest == BROADCAST || h.dest == myId)
    {
        bool enc = (h.flags & FLAG_ENC) != 0;
        uint8_t work[MAX_PL];
        memcpy(work, plPtr, cpLen);
        if (enc)
        {
            int ki = findPeer(h.origin);
            if (ki >= 0)
                crypt(work, cpLen, peers[ki].key, h);
            else if (h.dest == BROADCAST)
                crypt(work, cpLen, PUB_KEY, h);
            else
                decOk = false;
        }
        memcpy(txt, work, cpLen);
        txt[cpLen] = '\0';

        Serial.printf("RX origin=0x%04X dest=%s msgId=%d hops=%d/%d rssi=%.0f snr=%.1f enc=%d text=",
                      h.origin, h.dest == BROADCAST ? "broadcast" : String("0x" + String(h.dest, HEX)).c_str(),
                      h.msgId, h.hops, h.maxHops, rssi, snr, enc ? 1 : 0);
        if (enc && !decOk)
            Serial.println("<no key>");
        else
            Serial.println(txt);

        String pt = decOk ? String(txt) : String("");

        // ACK handling
        if (decOk && pt.startsWith(String(C_ACK)) && h.dest == myId)
        {
            int c1 = pt.indexOf(':', String(C_ACK).length());
            if (c1 > 0)
            {
                int c2 = pt.indexOf(':', c1 + 1);
                if (c2 > 0)
                {
                    uint16_t ao = (uint16_t)strtoul(pt.substring(c1 + 1, c2).c_str(), nullptr, 16);
                    uint16_t am = (uint16_t)pt.substring(c2 + 1).toInt();
                    removeOut(ao, am);
                }
            }
        }

        // Trace request arrived at destination
        if (!alreadySeen && decOk && pt.startsWith(String(C_TRACE_REQ)) && h.dest == myId && h.origin != myId)
        {
            String route = pt.substring(String(C_TRACE_REQ).length() + 1) + ">0x" + String(myId, HEX);
            sendTo(h.origin, String(C_TRACE_RESP) + ":" + route);
        }

        // Trace response
        if (decOk && pt.startsWith(String(C_TRACE_RESP)) && h.dest == myId)
        {
            Serial.printf("TRACEROUTE to 0x%04X: %s\n", h.origin, pt.substring(String(C_TRACE_RESP).length() + 1).c_str());
        }

        // Discovery
        if (!alreadySeen && decOk && pt == C_DISC_REQ && h.origin != myId)
            sendDiscResp(h.origin);
        else if (decOk && pt.startsWith(String(C_DISC_RESP)) && h.origin != myId)
        {
            int sc = pt.indexOf(':', String(C_DISC_RESP).length() + 1);
            uint16_t req = 0;
            if (sc > 0)
                req = (uint16_t)strtoul(pt.substring(sc + 1).c_str(), nullptr, 16);
            if (req == myId)
                Serial.printf("DISCOVERED 0x%04X hops=%d rssi=%.0f snr=%.1f\n", h.origin, h.hops, rssi, snr);
        }

        // Weather
        if (!alreadySeen && decOk && pt == C_WX_REQ && h.origin != myId && wxMode)
            sendWxResp(h.origin);
        else if (decOk && pt.startsWith(String(C_WX_DATA)) && h.origin != myId)
            Serial.printf("WEATHER from=0x%04X hops=%d %s\n", h.origin, h.hops, pt.c_str());

        // Auto-ACK directed user messages
        if (!alreadySeen && decOk && h.dest == myId && h.origin != myId && !pt.startsWith("#MESH_"))
            sendAck(h.origin, h.origin, h.msgId);
    }
    else
    {
        // Not for us — decode for image detection
        bool enc = (h.flags & FLAG_ENC) != 0;
        if (!enc)
        {
            memcpy(txt, plPtr, cpLen);
            txt[cpLen] = '\0';
        }
        else
        {
            uint8_t tmp[MAX_PL];
            memcpy(tmp, plPtr, cpLen);
            int ki = findPeer(h.origin);
            if (ki >= 0)
                crypt(tmp, cpLen, peers[ki].key, h);
            else
                crypt(tmp, cpLen, PUB_KEY, h);
            memcpy(txt, tmp, cpLen);
            txt[cpLen] = '\0';
            decOk = false;
        }
    }

    bool isImg = (strncmp(txt, "#MESH_IMG_S", 11) == 0 || strncmp(txt, "#MESH_IMG_C", 11) == 0);
    if (isImg)
        relayImg(h, plPtr, alreadySeen);
    else
        relay(h, plPtr, alreadySeen, decOk ? txt : nullptr);

    startRx();
}

// ── Serial command handler ──

void handleCmd(String line)
{
    line.trim();
    if (!line.length())
        return;

    if (line == "/help")
    {
        Serial.println(F(
            "/help             show commands\n"
            "/id               node ID\n"
            "/ttl <1..15>      set max hops\n"
            "/txpower <2..22>  set TX power\n"
            "/scan             discover nodes\n"
            "/scan deep        deep scan (15 hops)\n"
            "/stations         list known nodes\n"
            "/msg <id> <text>  direct message\n"
            "/pub <text>       public encrypted broadcast\n"
            "/eto <id> <text>  encrypted direct message\n"
            "/mykey gen|show|set <hex>  manage own key\n"
            "/key set <id> <hex>  store peer key\n"
            "/key del <id>     delete peer key\n"
            "/keys             list peer keys\n"
            "/traceroute <id>  trace route to node\n"
            "/wx on|off|status weather mode\n"
            "/wxreq [all|id]   request weather\n"
            "/reliable on|off  toggle reliable send\n"
            "/buffer           show outbound buffer\n"
            "/save             force save settings\n"
            "/settings         dump settings JSON\n"
            "<text>            broadcast message"));
        return;
    }

    if (line == "/settings")
    {
        Serial.printf("{\"nodeId\":\"0x%04X\",\"maxHops\":%d,\"wxMode\":%s,\"txPower\":%d,"
                      "\"reliable\":%s,\"myKeyValid\":%s",
                      myId, maxHops, wxMode ? "true" : "false", txPwr,
                      reliable ? "true" : "false", myKeyValid ? "true" : "false");
        if (myKeyValid)
            Serial.printf(",\"myKey\":\"%s\"", keyHex(myKey).c_str());
        int nk = 0, ns = 0, nb = 0;
        for (uint8_t i = 0; i < KEY_SZ; i++)
            if (peers[i].valid)
                nk++;
        for (uint8_t i = 0; i < STA_SZ; i++)
            if (sta[i].node)
                ns++;
        for (uint8_t i = 0; i < OUTBUF_SZ; i++)
            if (outbuf[i].active)
                nb++;
        Serial.printf(",\"peerKeys\":%d,\"stations\":%d,\"outbound\":%d}\n", nk, ns, nb);
        return;
    }

    if (line == "/id")
    {
        Serial.printf("Node ID: 0x%04X\n", myId);
        return;
    }

    if (line.startsWith("/ttl "))
    {
        int v = line.substring(5).toInt();
        if (v >= 1 && v <= 15)
        {
            maxHops = v;
            markDirty();
            Serial.printf("maxHops=%d\n", maxHops);
        }
        else
            Serial.println("Invalid (1..15)");
        return;
    }

    if (line.startsWith("/txpower "))
    {
        int v = line.substring(9).toInt();
        if (v < 2)
        {
            Serial.println("Invalid (2..22)");
            return;
        }
        if (v > MAX_PWR)
            v = MAX_PWR;
        txPwr = (int8_t)v;
        radio.setOutputPower(txPwr);
        markDirty();
        Serial.printf("TX power: %d dBm\n", txPwr);
        return;
    }

    if (line == "/scan")
    {
        sendDiscReq();
        Serial.println("Scan sent");
        return;
    }

    if (line == "/scan deep")
    {
        String p = String(C_DISC_REQ);
        MeshHeader h;
        h.magic = MAGIC;
        h.version = VER;
        h.origin = myId;
        h.msgId = msgSeq++;
        h.dest = BROADCAST;
        h.hops = 0;
        h.maxHops = 15;
        h.flags = 0;
        h.plLen = (uint8_t)p.length();
        uint8_t buf[MAX_PL];
        memcpy(buf, p.c_str(), h.plLen);
        markSeen(h.origin, h.msgId);
        sendFrame(h, buf);
        Serial.println("Deep scan sent (15 hops)");
        return;
    }

    if (line == "/stations")
    {
        printSta();
        return;
    }

    if (line == "/wx on")
    {
        wxMode = true;
        markDirty();
        Serial.println("WX: ON");
        return;
    }
    if (line == "/wx off")
    {
        wxMode = false;
        markDirty();
        Serial.println("WX: OFF");
        return;
    }
    if (line == "/wx status")
    {
        Serial.printf("WX: %s\n", wxMode ? "ON" : "OFF");
        return;
    }

    if (line == "/wxreq" || line == "/wxreq all")
    {
        sendTo(BROADCAST, String(C_WX_REQ));
        Serial.println("WX request broadcast");
        return;
    }
    if (line.startsWith("/wxreq "))
    {
        uint16_t n = 0;
        if (parseNode(line.substring(7), n))
            sendTo(n, String(C_WX_REQ));
        else
            Serial.println("Invalid node");
        return;
    }

    if (line == "/mykey gen")
    {
        genKey();
        markDirty();
        Serial.printf("Key generated. Share: /key set 0x%04X %s\n", myId, keyHex(myKey).c_str());
        return;
    }
    if (line.startsWith("/mykey set "))
    {
        uint8_t k[KEY_LEN];
        if (parseHex(line.substring(11), k))
        {
            memcpy(myKey, k, KEY_LEN);
            myKeyValid = true;
            markDirty();
            Serial.printf("Key set: %s\n", keyHex(myKey).c_str());
        }
        else
            Serial.println("Invalid hex (32 chars)");
        return;
    }
    if (line == "/mykey show")
    {
        if (myKeyValid)
            Serial.printf("Key 0x%04X: %s\n", myId, keyHex(myKey).c_str());
        else
            Serial.println("No key. Use /mykey gen");
        return;
    }

    if (line == "/keys")
    {
        printKeys();
        return;
    }

    if (line.startsWith("/key set "))
    {
        String r = line.substring(9);
        r.trim();
        int sp = r.indexOf(' ');
        if (sp <= 0)
        {
            Serial.println("Syntax: /key set <id> <hex32>");
            return;
        }
        uint16_t n = 0;
        uint8_t k[KEY_LEN];
        if (!parseNode(r.substring(0, sp), n))
        {
            Serial.println("Invalid node");
            return;
        }
        if (!parseHex(r.substring(sp + 1), k))
        {
            Serial.println("Invalid hex");
            return;
        }
        if (setPeer(n, k))
        {
            markDirty();
            Serial.printf("Key stored for 0x%04X\n", n);
        }
        else
            Serial.println("Key store full");
        return;
    }

    if (line.startsWith("/key del "))
    {
        uint16_t n = 0;
        if (parseNode(line.substring(9), n))
        {
            if (delPeer(n))
            {
                markDirty();
                Serial.printf("Key deleted 0x%04X\n", n);
            }
            else
                Serial.println("No key for node");
        }
        else
            Serial.println("Invalid node");
        return;
    }

    if (line.startsWith("/msg "))
    {
        String r = line.substring(5);
        r.trim();
        int sp = r.indexOf(' ');
        if (sp <= 0)
        {
            Serial.println("Syntax: /msg <id> <text>");
            return;
        }
        uint16_t n = 0;
        if (!parseNode(r.substring(0, sp), n))
        {
            Serial.println("Invalid node");
            return;
        }
        String t = r.substring(sp + 1);
        t.trim();
        if (sendTo(n, t))
            Serial.printf("TX to=0x%04X msgId=%d text=%s\n", n, msgSeq - 1, t.c_str());
        return;
    }

    if (line.startsWith("/pub "))
    {
        String t = line.substring(5);
        t.trim();
        if (!t.length())
        {
            Serial.println("Syntax: /pub <text>");
            return;
        }
        // Public key encrypted broadcast
        MeshHeader h;
        h.magic = MAGIC;
        h.version = VER;
        h.origin = myId;
        h.msgId = msgSeq++;
        h.dest = BROADCAST;
        h.hops = 0;
        h.maxHops = maxHops;
        h.flags = FLAG_ENC;
        String p = t.substring(0, MAX_PL);
        h.plLen = (uint8_t)p.length();
        uint8_t buf[MAX_PL];
        memcpy(buf, p.c_str(), h.plLen);
        crypt(buf, h.plLen, PUB_KEY, h);
        markSeen(h.origin, h.msgId);
        if (sendFrame(h, buf))
            Serial.printf("TX enc=pub msgId=%d text=%s\n", msgSeq - 1, p.c_str());
        return;
    }

    if (line.startsWith("/eto "))
    {
        String r = line.substring(5);
        r.trim();
        int sp = r.indexOf(' ');
        if (sp <= 0)
        {
            Serial.println("Syntax: /eto <id> <text>");
            return;
        }
        uint16_t n = 0;
        if (!parseNode(r.substring(0, sp), n))
        {
            Serial.println("Invalid node");
            return;
        }
        if (n == BROADCAST)
        {
            Serial.println("Encrypted only for direct");
            return;
        }
        String t = r.substring(sp + 1);
        t.trim();
        if (sendTo(n, t, true))
            Serial.printf("ETX to=0x%04X msgId=%d text=%s\n", n, msgSeq - 1, t.c_str());
        return;
    }

    if (line.startsWith("/traceroute "))
    {
        uint16_t n = 0;
        if (parseNode(line.substring(12), n))
            sendTrace(n);
        else
            Serial.println("Invalid node");
        return;
    }

    if (line == "/reliable on")
    {
        reliable = true;
        markDirty();
        Serial.println("Reliable: ON");
        return;
    }
    if (line == "/reliable off")
    {
        reliable = false;
        for (uint8_t i = 0; i < OUTBUF_SZ; i++)
            outbuf[i].active = false;
        markDirty();
        Serial.println("Reliable: OFF (cleared)");
        return;
    }
    if (line == "/reliable status")
    {
        Serial.printf("Reliable: %s\n", reliable ? "ON" : "OFF");
        return;
    }

    if (line == "/buffer")
    {
        Serial.println("Outbound:");
        bool any = false;
        for (uint8_t i = 0; i < OUTBUF_SZ; i++)
        {
            if (!outbuf[i].active)
                continue;
            any = true;
            Serial.printf("- msgId=%d to=0x%04X retries=%d/%d\n",
                          outbuf[i].hdr.msgId, outbuf[i].hdr.dest, outbuf[i].retries, MAX_RETRY);
        }
        if (!any)
            Serial.println("(empty)");
        return;
    }

    if (line == "/save")
    {
        saveFlash();
        return;
    }

    // Default: broadcast as user message
    if (sendTo(BROADCAST, line))
        Serial.printf("TX msgId=%d hops=0/%d text=%s\n", msgSeq - 1, maxHops, line.c_str());
}

// ── Serial reader ──

void readSerial()
{
    while (Serial.available())
    {
        char c = (char)Serial.read();
        if (c == '\r')
            continue;
        if (c == '\n')
        {
            String l = serBuf;
            serBuf = "";
            handleCmd(l);
            continue;
        }
        if (serBuf.length() < MAX_PL)
            serBuf += c;
    }
}

// ── Setup ──

void setup()
{
    Serial.begin(115200);
    {
        uint32_t t = millis();
        while (!Serial && millis() - t < 3000)
            delay(10);
    }
    delay(200);

    uint64_t mac = ESP.getEfuseMac();
    uint16_t macId = (uint16_t)((mac ^ (mac >> 16) ^ (mac >> 32)) & 0xFFFF);
    // alternative for known issues with specific devices no problem if it has functioning nvs
    if (macId == 0 || macId == BROADCAST)
        macId = (uint16_t)((esp_random() & 0xFFFE) | 1);
    randomSeed((unsigned long)micros() ^ (unsigned long)macId);

    bool loaded = loadFlash();
    if (loaded)
    {
        if (myId == 0)
        {
            myId = macId;
            markDirty();
        }
        Serial.println("[NVS] restored");
    }
    else
    {
        myId = macId;
        saveFlash();
        Serial.println("[NVS] first boot");
    }

    SPI.begin(PIN_SCK, PIN_MISO, PIN_MOSI, PIN_NSS);
    int16_t st = radio.begin(FREQ, BW, SF, CR, SYNC, MAX_PWR, PREAMBLE, TCXO_V, false);
    if (st != RADIOLIB_ERR_NONE)
    {
        Serial.printf("LoRa init error: %d\n", st);
        while (true)
        {
            delay(5000);
            Serial.printf("[STUCK] LoRa err: %d\n", st);
        }
    }

    radio.setDio1Action(onRx);
    radio.setOutputPower(txPwr);
    startRx();

    Serial.printf("MegaMesh Generic started\nNode: 0x%04X  hops=%d  txPwr=%d  reliable=%s\n",
                  myId, maxHops, txPwr, reliable ? "on" : "off");
    Serial.println("Type /help for commands");
}

// ── Loop ──

void loop()
{
    readSerial();
    saveIfNeeded();

    if (rxFlag)
    {
        rxFlag = false;
        handleRx();
    }

    static uint32_t lastTick = 0;
    uint32_t now = millis();
    if (now - lastTick >= OUTBOUND_TICK)
    {
        lastTick = now;
        processOut();
        processImg();
    }

    delay(2);
}
