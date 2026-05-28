#include <Arduino.h>
#include <Wire.h>

#if __has_include(<DHT.h>)
#include <DHT.h>
#define HAS_DHT22_LIB 1
#else
#define HAS_DHT22_LIB 0
#endif

#if __has_include(<Adafruit_BMP280.h>)
#include <Adafruit_BMP280.h>
#define HAS_BMP280_LIB 1
#else
#define HAS_BMP280_LIB 0
#endif

// Heltec LoRa 32 V4 sensor pins
static const uint8_t PIN_DHT22 = 45;
static const uint8_t PIN_BMP_SDA = 20;
static const uint8_t PIN_BMP_SCL = 21;
static const uint8_t BMP280_ADDR_PRIMARY = 0x76;
static const uint8_t BMP280_ADDR_SECONDARY = 0x77;

static const uint32_t WX_READ_INTERVAL_MS = 2500;

bool dhtReady = false;
bool bmpReady = false;
float lastTempC = NAN;
float lastHumidity = NAN;
float lastPressureHpa = NAN;
uint32_t lastWxReadAt = 0;
uint16_t fakeNodeId = 0xBEEF;
bool autoRead = true;

TwoWire bmpWire(1);

#if HAS_DHT22_LIB
DHT dht(PIN_DHT22, DHT11);
#endif

#if HAS_BMP280_LIB
Adafruit_BMP280 bmp;
#endif

void printLibStatus()
{
    Serial.print("DHT lib: ");
    Serial.println(HAS_DHT22_LIB ? "OK" : "MISSING");
    Serial.print("BMP280 lib: ");
    Serial.println(HAS_BMP280_LIB ? "OK" : "MISSING");
}

void initSensors()
{
#if HAS_DHT22_LIB
    dht.begin();
    dhtReady = true;
#else
    dhtReady = false;
#endif

#if HAS_BMP280_LIB
    if (bmp.begin(BMP280_ADDR_PRIMARY, &bmpWire) || bmp.begin(BMP280_ADDR_SECONDARY, &bmpWire))
    {
        bmpReady = true;
    }
    else
    {
        bmpReady = false;
        Serial.println("[WX] BMP280 nicht gefunden (0x76/0x77)");
    }
#else
    bmpReady = false;
#endif
}

void updateWeatherReadings(bool force)
{
    const uint32_t nowMs = millis();
    if (!force && nowMs - lastWxReadAt < WX_READ_INTERVAL_MS)
    {
        return;
    }
    lastWxReadAt = nowMs;

#if HAS_DHT22_LIB
    if (dhtReady)
    {
        float t = dht.readTemperature();
        float h = dht.readHumidity();
        if (!isnan(t))
        {
            lastTempC = t;
        }
        if (!isnan(h))
        {
            lastHumidity = h;
        }
    }
#endif

#if HAS_BMP280_LIB
    if (bmpReady)
    {
        float bt = bmp.readTemperature();
        float bp = bmp.readPressure() / 100.0f;
        if (isnan(lastTempC) && !isnan(bt))
        {
            lastTempC = bt;
        }
        if (!isnan(bp))
        {
            lastPressureHpa = bp;
        }
    }
#endif
}

String buildWeatherPayload()
{
    float temperature = isnan(lastTempC) ? 0.0f : lastTempC;
    float humidity = isnan(lastHumidity) ? 0.0f : lastHumidity;
    float pressure = isnan(lastPressureHpa) ? 0.0f : lastPressureHpa;

    String payload = "#MESH_WX_DATA";
    payload += ":node=0x";
    payload += String(fakeNodeId, HEX);
    payload += ",tempC=";
    payload += String(temperature, 1);
    payload += ",hum=";
    payload += String(humidity, 1);
    payload += ",hPa=";
    payload += String(pressure, 1);
    payload += ",tempOk=";
    payload += isnan(lastTempC) ? "0" : "1";
    payload += ",humOk=";
    payload += isnan(lastHumidity) ? "0" : "1";
    payload += ",pressOk=";
    payload += isnan(lastPressureHpa) ? "0" : "1";
    return payload;
}

void printReadings()
{
    Serial.print("tempC=");
    if (isnan(lastTempC))
        Serial.print("nan");
    else
        Serial.print(lastTempC, 1);

    Serial.print(" hum=");
    if (isnan(lastHumidity))
        Serial.print("nan");
    else
        Serial.print(lastHumidity, 1);

    Serial.print(" hPa=");
    if (isnan(lastPressureHpa))
        Serial.print("nan");
    else
        Serial.print(lastPressureHpa, 1);

    Serial.print(" | flags T/H/P=");
    Serial.print(isnan(lastTempC) ? 0 : 1);
    Serial.print("/");
    Serial.print(isnan(lastHumidity) ? 0 : 1);
    Serial.print("/");
    Serial.println(isnan(lastPressureHpa) ? 0 : 1);
}

void printHelp()
{
    Serial.println("Commands:");
    Serial.println("  read          -> force sensor read + print values");
    Serial.println("  packet        -> print payload as firmware would send");
    Serial.println("  req           -> simulate incoming #MESH_WX_REQ and print outgoing payload");
    Serial.println("  auto on|off   -> periodic reads every 2.5s");
    Serial.println("  status        -> print sensor and lib status");
    Serial.println("  help          -> show commands");
}

void handleCommand(String line)
{
    line.trim();
    if (line.length() == 0)
    {
        return;
    }

    if (line == "help")
    {
        printHelp();
        return;
    }

    if (line == "status")
    {
        printLibStatus();
        Serial.print("DHT sensor: ");
        Serial.println(dhtReady ? "READY" : "NOT READY");
        Serial.print("BMP sensor: ");
        Serial.println(bmpReady ? "READY" : "NOT READY");
        return;
    }

    if (line == "read")
    {
        updateWeatherReadings(true);
        printReadings();
        return;
    }

    if (line == "packet")
    {
        updateWeatherReadings(true);
        Serial.print("payload=");
        Serial.println(buildWeatherPayload());
        return;
    }

    if (line == "req")
    {
        updateWeatherReadings(true);
        String payload = buildWeatherPayload();
        Serial.println("REQ simulated: would send WX response with payload:");
        Serial.println(payload);
        return;
    }

    if (line == "auto on")
    {
        autoRead = true;
        Serial.println("auto read ON");
        return;
    }

    if (line == "auto off")
    {
        autoRead = false;
        Serial.println("auto read OFF");
        return;
    }

    Serial.println("Unknown command. Type 'help'.");
}

void setup()
{
    Serial.begin(115200);
    delay(300);

    bmpWire.begin(PIN_BMP_SDA, PIN_BMP_SCL);

    printLibStatus();
    initSensors();
    printHelp();
}

void loop()
{
    if (Serial.available())
    {
        String line = Serial.readStringUntil('\n');
        handleCommand(line);
    }

    if (autoRead)
    {
        uint32_t before = lastWxReadAt;
        updateWeatherReadings(false);
        if (lastWxReadAt != before)
        {
            printReadings();
        }
    }

    delay(10);
}
