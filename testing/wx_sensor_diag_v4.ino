#include <Arduino.h>
#include <Wire.h>

// Heltec LoRa 32 V4 sensor pins
static const uint8_t PIN_DHT22 = 45;
static const uint8_t PIN_BMP_SDA = 48;
static const uint8_t PIN_BMP_SCL = 47;
static const uint8_t BMP280_ADDR_PRIMARY = 0x76;
static const uint8_t BMP280_ADDR_SECONDARY = 0x77;
static const uint16_t DHT22_START_LOW_US = 2000;
static const uint16_t DHT22_START_HIGH_US = 40;
static const uint8_t BMP280_CONFIG_STANDBY_1000MS = 0xA0;
static const uint8_t BMP280_CTRL_NORMAL_X1 = 0x27;

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

struct Bmp280Calibration
{
    uint16_t digT1 = 0;
    int16_t digT2 = 0;
    int16_t digT3 = 0;
    uint16_t digP1 = 0;
    int16_t digP2 = 0;
    int16_t digP3 = 0;
    int16_t digP4 = 0;
    int16_t digP5 = 0;
    int16_t digP6 = 0;
    int16_t digP7 = 0;
    int16_t digP8 = 0;
    int16_t digP9 = 0;
    int32_t tFine = 0;
    bool valid = false;
};

Bmp280Calibration bmpCal;
uint8_t bmpAddress = 0;

void printLibStatus()
{
    Serial.println("DHT lib: INTERNAL");
    Serial.println("BMP280 lib: INTERNAL");
}

static bool readBytesFromWire(TwoWire &wireBus, uint8_t address, uint8_t reg, uint8_t *buffer, size_t len)
{
    wireBus.beginTransmission(address);
    wireBus.write(reg);
    if (wireBus.endTransmission(false) != 0)
    {
        return false;
    }

    size_t received = wireBus.requestFrom(static_cast<int>(address), static_cast<int>(len));
    if (received != len)
    {
        return false;
    }

    for (size_t i = 0; i < len; ++i)
    {
        buffer[i] = wireBus.read();
    }
    return true;
}

static bool writeByteToWire(TwoWire &wireBus, uint8_t address, uint8_t reg, uint8_t value)
{
    wireBus.beginTransmission(address);
    wireBus.write(reg);
    wireBus.write(value);
    return wireBus.endTransmission() == 0;
}

static bool initBmp280()
{
    static const uint8_t BMP280_REG_ID = 0xD0;
    static const uint8_t BMP280_REG_CALIB = 0x88;
    static const uint8_t BMP280_REG_CONFIG = 0xF5;
    static const uint8_t BMP280_REG_CTRL_MEAS = 0xF4;
    static const uint8_t BMP280_CHIP_ID = 0x58;

    const uint8_t addresses[] = {BMP280_ADDR_PRIMARY, BMP280_ADDR_SECONDARY};
    uint8_t chipId = 0;

    bmpReady = false;
    bmpCal.valid = false;
    bmpAddress = 0;
    bmpWire.begin(PIN_BMP_SDA, PIN_BMP_SCL);

    for (uint8_t address : addresses)
    {
        if (!readBytesFromWire(bmpWire, address, BMP280_REG_ID, &chipId, 1))
        {
            continue;
        }

        if (chipId == BMP280_CHIP_ID)
        {
            bmpAddress = address;
            break;
        }
    }

    if (bmpAddress == 0)
    {
        return false;
    }

    uint8_t calib[24];
    if (!readBytesFromWire(bmpWire, bmpAddress, BMP280_REG_CALIB, calib, sizeof(calib)))
    {
        return false;
    }

    bmpCal.digT1 = static_cast<uint16_t>(calib[1] << 8 | calib[0]);
    bmpCal.digT2 = static_cast<int16_t>(calib[3] << 8 | calib[2]);
    bmpCal.digT3 = static_cast<int16_t>(calib[5] << 8 | calib[4]);
    bmpCal.digP1 = static_cast<uint16_t>(calib[7] << 8 | calib[6]);
    bmpCal.digP2 = static_cast<int16_t>(calib[9] << 8 | calib[8]);
    bmpCal.digP3 = static_cast<int16_t>(calib[11] << 8 | calib[10]);
    bmpCal.digP4 = static_cast<int16_t>(calib[13] << 8 | calib[12]);
    bmpCal.digP5 = static_cast<int16_t>(calib[15] << 8 | calib[14]);
    bmpCal.digP6 = static_cast<int16_t>(calib[17] << 8 | calib[16]);
    bmpCal.digP7 = static_cast<int16_t>(calib[19] << 8 | calib[18]);
    bmpCal.digP8 = static_cast<int16_t>(calib[21] << 8 | calib[20]);
    bmpCal.digP9 = static_cast<int16_t>(calib[23] << 8 | calib[22]);
    bmpCal.valid = bmpCal.digP1 != 0;

    if (!bmpCal.valid)
    {
        return false;
    }

    if (!writeByteToWire(bmpWire, bmpAddress, BMP280_REG_CONFIG, BMP280_CONFIG_STANDBY_1000MS) ||
        !writeByteToWire(bmpWire, bmpAddress, BMP280_REG_CTRL_MEAS, BMP280_CTRL_NORMAL_X1))
    {
        bmpCal.valid = false;
        return false;
    }

    bmpReady = true;
    return true;
}

static bool readBmp280Values(float &temperatureC, float &pressureHpa)
{
    static const uint8_t BMP280_REG_DATA = 0xF7;

    if (!bmpReady || !bmpCal.valid || bmpAddress == 0)
    {
        return false;
    }

    uint8_t data[6];
    if (!readBytesFromWire(bmpWire, bmpAddress, BMP280_REG_DATA, data, sizeof(data)))
    {
        bmpReady = false;
        bmpCal.valid = false;
        return false;
    }

    int32_t rawPressure = (static_cast<int32_t>(data[0]) << 12) |
                          (static_cast<int32_t>(data[1]) << 4) |
                          (static_cast<int32_t>(data[2]) >> 4);
    int32_t rawTemperature = (static_cast<int32_t>(data[3]) << 12) |
                             (static_cast<int32_t>(data[4]) << 4) |
                             (static_cast<int32_t>(data[5]) >> 4);

    int32_t var1 = ((((rawTemperature >> 3) - (static_cast<int32_t>(bmpCal.digT1) << 1))) *
                    static_cast<int32_t>(bmpCal.digT2)) >>
                   11;
    int32_t var2 = (((((rawTemperature >> 4) - static_cast<int32_t>(bmpCal.digT1)) *
                      ((rawTemperature >> 4) - static_cast<int32_t>(bmpCal.digT1))) >>
                     12) *
                    static_cast<int32_t>(bmpCal.digT3)) >>
                   14;
    bmpCal.tFine = var1 + var2;
    int32_t t = (bmpCal.tFine * 5 + 128) >> 8;
    temperatureC = t / 100.0f;

    int64_t pVar1 = static_cast<int64_t>(bmpCal.tFine) - 128000;
    int64_t pVar2 = pVar1 * pVar1 * static_cast<int64_t>(bmpCal.digP6);
    pVar2 += (pVar1 * static_cast<int64_t>(bmpCal.digP5)) << 17;
    pVar2 += static_cast<int64_t>(bmpCal.digP4) << 35;
    pVar1 = ((pVar1 * pVar1 * static_cast<int64_t>(bmpCal.digP3)) >> 8) +
            ((pVar1 * static_cast<int64_t>(bmpCal.digP2)) << 12);
    pVar1 = ((((static_cast<int64_t>(1) << 47) + pVar1)) * static_cast<int64_t>(bmpCal.digP1)) >> 33;

    if (pVar1 == 0)
    {
        return false;
    }

    int64_t pressure = 1048576 - rawPressure;
    pressure = (((pressure << 31) - pVar2) * 3125) / pVar1;
    pVar1 = (static_cast<int64_t>(bmpCal.digP9) * (pressure >> 13) * (pressure >> 13)) >> 25;
    pVar2 = (static_cast<int64_t>(bmpCal.digP8) * pressure) >> 19;
    pressure = ((pressure + pVar1 + pVar2) >> 8) + (static_cast<int64_t>(bmpCal.digP7) << 4);
    pressureHpa = (pressure / 256.0f) / 100.0f;
    return true;
}

static bool readDht22Values(float &temperatureC, float &humidityPct)
{
    uint8_t data[5] = {0, 0, 0, 0, 0};

    pinMode(PIN_DHT22, OUTPUT);
    digitalWrite(PIN_DHT22, LOW);
    delayMicroseconds(DHT22_START_LOW_US);
    digitalWrite(PIN_DHT22, HIGH);
    delayMicroseconds(DHT22_START_HIGH_US);
    pinMode(PIN_DHT22, INPUT_PULLUP);

    if (pulseIn(PIN_DHT22, LOW, 120) == 0 || pulseIn(PIN_DHT22, HIGH, 120) == 0)
    {
        return false;
    }

    for (uint8_t i = 0; i < 40; ++i)
    {
        if (pulseIn(PIN_DHT22, LOW, 100) == 0)
        {
            return false;
        }

        uint32_t highTime = pulseIn(PIN_DHT22, HIGH, 150);
        if (highTime == 0)
        {
            return false;
        }

        data[i / 8] <<= 1;
        if (highTime > 40)
        {
            data[i / 8] |= 1;
        }
    }

    uint16_t checksumSum = static_cast<uint16_t>(data[0]) + static_cast<uint16_t>(data[1]) +
                           static_cast<uint16_t>(data[2]) + static_cast<uint16_t>(data[3]);
    uint8_t checksum = static_cast<uint8_t>(checksumSum & 0xFF);
    if (checksum != data[4])
    {
        return false;
    }

    uint16_t rawHumidity = static_cast<uint16_t>(data[0] << 8 | data[1]);
    uint16_t rawTemp = static_cast<uint16_t>((data[2] & 0x7F) << 8 | data[3]);

    humidityPct = rawHumidity * 0.1f;
    temperatureC = rawTemp * 0.1f;
    if (data[2] & 0x80)
    {
        temperatureC = -temperatureC;
    }

    return true;
}

void initSensors()
{
    dhtReady = false;
    pinMode(PIN_DHT22, INPUT_PULLUP);

    if (!initBmp280())
    {
        Serial.println("[WX] BMP280 nicht gefunden (0x76/0x77 auf Pins 48/47)");
    }
}

void updateWeatherReadings(bool force)
{
    const uint32_t nowMs = millis();
    if (!force && nowMs - lastWxReadAt < WX_READ_INTERVAL_MS)
    {
        return;
    }
    lastWxReadAt = nowMs;

    float dhtTemp = NAN;
    float dhtHum = NAN;
    if (readDht22Values(dhtTemp, dhtHum))
    {
        dhtReady = true;
        lastTempC = dhtTemp;
        lastHumidity = dhtHum;
    }
    else
    {
        dhtReady = false;
    }

    if (!bmpReady)
    {
        initBmp280();
    }

    float bt = NAN;
    float bp = NAN;
    if (readBmp280Values(bt, bp))
    {
        bmpReady = true;
        if (isnan(lastTempC) && !isnan(bt))
        {
            lastTempC = bt;
        }
        if (!isnan(bp))
        {
            lastPressureHpa = bp;
        }
    }
    else
    {
        bmpReady = false;
    }
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
    Serial.println("  status        -> print sensor status");
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
        Serial.print("DHT22 sensor: ");
        Serial.println(dhtReady ? "READY" : "NOT READY");
        Serial.print("BMP280 sensor: ");
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

    printLibStatus();
    initSensors();
    updateWeatherReadings(true);
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
