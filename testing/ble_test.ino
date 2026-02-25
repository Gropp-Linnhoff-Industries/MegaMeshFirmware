#include <Arduino.h>
#include <BLEDevice.h>
#include <BLEUtils.h>
#include <BLEServer.h>

static BLEUUID serviceUUID("6E400001-B5A3-F393-E0A9-E50E24DCCA9E");
static BLEUUID charRXUUID("6E400002-B5A3-F393-E0A9-E50E24DCCA9E");
static BLEUUID charTXUUID("6E400003-B5A3-F393-E0A9-E50E24DCCA9E");

BLECharacteristic *pTX = nullptr;

class RXCallbacks : public BLECharacteristicCallbacks
{
    void onWrite(BLECharacteristic *pCharacteristic) override
    {
        auto tmp = pCharacteristic->getValue();
        String s = String(tmp.c_str());
        if (s.length() > 0)
        {
            Serial.print("BLE RX: ");
            Serial.println(s);

            if (pTX)
                pTX->setValue(s);
            if (pTX)
                pTX->notify();
        }
    }
};

void setup()
{
    Serial.begin(115200);
    delay(500);
    Serial.println("BLE test: init");

    uint64_t mac = ESP.getEfuseMac();
    uint16_t node = (uint16_t)(mac & 0xFFFF);
    uint32_t base = 0x6E400000 | (uint32_t)node;
    char svc[64];
    char rx[64];
    char tx[64];
    sprintf(svc, "%08X-B5A3-F393-E0A9-E50E24DCCA9E", base | 0x0001);
    sprintf(rx, "%08X-B5A3-F393-E0A9-E50E24DCCA9E", base | 0x0002);
    sprintf(tx, "%08X-B5A3-F393-E0A9-E50E24DCCA9E", base | 0x0003);

    BLEUUID serviceUUID(svc);
    BLEUUID charRXUUID(rx);
    BLEUUID charTXUUID(tx);

    String devName = String("ESP32-BLE-Test-") + String(node, HEX);
    BLEDevice::init(devName.c_str());
    BLEServer *pServer = BLEDevice::createServer();
    BLEService *pService = pServer->createService(serviceUUID);

    BLECharacteristic *pRX = pService->createCharacteristic(
        charRXUUID,
        BLECharacteristic::PROPERTY_WRITE);
    pRX->setCallbacks(new RXCallbacks());

    pTX = pService->createCharacteristic(
        charTXUUID,
        BLECharacteristic::PROPERTY_NOTIFY);
    pTX->addDescriptor(new BLE2902());

    pService->start();

    BLEAdvertising *pAdvertising = BLEDevice::getAdvertising();
    pAdvertising->addServiceUUID(serviceUUID);
    pAdvertising->setScanResponse(true);
    pAdvertising->start();

    Serial.print("BLE advertising started as '");
    Serial.print(devName);
    Serial.println("'");
}

void loop()
{
    delay(1000);
}
