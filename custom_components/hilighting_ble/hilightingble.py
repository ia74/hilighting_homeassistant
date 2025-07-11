import asyncio
import logging
from typing import Any, TypeVar, cast, Tuple
from collections.abc import Callable

from homeassistant.components import bluetooth
from homeassistant.components.bluetooth import (
    BluetoothServiceInfoBleak,
    async_ble_device_from_address,
)
from homeassistant.components.light import ColorMode, EFFECT_OFF
from homeassistant.exceptions import ConfigEntryNotReady

from bleak.backends.device import BLEDevice
from bleak.backends.service import BleakGATTCharacteristic, BleakGATTServiceCollection
from bleak.exc import BleakDBusError
from bleak_retry_connector import (
    BLEAK_RETRY_EXCEPTIONS as BLEAK_EXCEPTIONS,
    BleakClientWithServiceCache,
    BleakNotFoundError,
    establish_connection,
)

LOGGER = logging.getLogger(__name__)

EFFECT_MAP = {f"Effect {i}": i for i in range(10)}
EFFECT_LIST = sorted(EFFECT_MAP)
EFFECT_ID_TO_NAME = {v: k for k, v in EFFECT_MAP.items()}

WRITE_CHARACTERISTIC_UUID = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"
FIRMWARE_REVISION_UUID = "00002a26-0000-1000-8000-00805f9b34fb"
SW_NUMBER_UUID = "00002a28-0000-1000-8000-00805f9b34fb"
MANUFACTURER_NAME_UUID = "00002a29-0000-1000-8000-00805f9b34fb"
DEFAULT_ATTEMPTS = 3
BLEAK_BACKOFF_TIME = 0.25
RETRY_BACKOFF_EXCEPTIONS = (BleakDBusError,)

WrapFuncType = TypeVar("WrapFuncType", bound=Callable[..., Any])

def retry_bluetooth_connection_error(func: WrapFuncType) -> WrapFuncType:
    async def _async_wrap(self: "HILIGHTINGInstance", *args: Any, **kwargs: Any) -> Any:
        for attempt in range(DEFAULT_ATTEMPTS):
            try:
                return await func(self, *args, **kwargs)
            except BleakNotFoundError:
                raise
            except RETRY_BACKOFF_EXCEPTIONS as err:
                if attempt == DEFAULT_ATTEMPTS - 1:
                    LOGGER.error("%s: Max retries reached on %s: %s", self.name, func.__name__, err)
                    raise
                LOGGER.warning("%s: Retry %s/%s on %s due to %s", self.name, attempt+1, DEFAULT_ATTEMPTS, func.__name__, err)
                await asyncio.sleep(BLEAK_BACKOFF_TIME)
            except BLEAK_EXCEPTIONS as err:
                if attempt == DEFAULT_ATTEMPTS - 1:
                    LOGGER.error("%s: Max retries reached on %s: %s", self.name, func.__name__, err)
                    raise
                LOGGER.warning("%s: BLE error retry %s/%s on %s: %s", self.name, attempt+1, DEFAULT_ATTEMPTS, func.__name__, err)
                await asyncio.sleep(BLEAK_BACKOFF_TIME)

    return cast(WrapFuncType, _async_wrap)


class HILIGHTINGInstance:
    def __init__(
        self,
        hass,
        service_info: BluetoothServiceInfoBleak,
        delay: int = 30,
        data: dict = {},
        options: dict = {},
    ):
        self._hass = hass
        self._service_info = service_info
        self._mac = service_info.address
        self._name = service_info.name or f"HILIGHTING-{self._mac[-5:]}"
        self._delay = delay
        self._data = data
        self._options = options

        self._device: BLEDevice | None = async_ble_device_from_address(hass, self._mac, connectable=True)
        if not self._device:
            raise ConfigEntryNotReady(f"Device with address {self._mac} not found via Home Assistant Bluetooth integration.")

        self.loop = asyncio.get_running_loop()
        self._connect_lock = asyncio.Lock()
        self._client: BleakClientWithServiceCache | None = None
        self._cached_services: BleakGATTServiceCollection | None = None
        self._expected_disconnect = False
        self._disconnect_timer: asyncio.TimerHandle | None = None

        self._is_on = False
        self._rgb_color = (255, 255, 255)
        self._brightness = 255
        self._effect = EFFECT_OFF
        self._color_mode = ColorMode.RGB

        self._write_uuid = None
        self._manufacturer_name_char = None
        self._firmware_revision_char = None
        self._model_number_char = None

        self._turn_on_cmd = bytearray.fromhex("55 01 02 01")
        self._turn_off_cmd = bytearray.fromhex("55 01 02 00")

        self._model = data.get("model")
        self._manufacturer_name = data.get("manufacturer_name")
        self._firmware_version = data.get("firmware_version")

    @property
    def name(self):
        return self._name

    @property
    def mac(self):
        return self._mac

    @property
    def is_on(self):
        return self._is_on

    @property
    def brightness(self):
        return self._brightness

    @property
    def rgb_color(self):
        return self._rgb_color

    @property
    def effect_list(self):
        return EFFECT_LIST

    @property
    def effect(self):
        return self._effect

    @property
    def color_mode(self):
        return self._color_mode

    @retry_bluetooth_connection_error
    async def turn_on(self):
        await self._write(self._turn_on_cmd)
        self._is_on = True

    @retry_bluetooth_connection_error
    async def turn_off(self):
        await self._write(self._turn_off_cmd)
        self._is_on = False

    @retry_bluetooth_connection_error
    async def set_rgb_color(self, rgb: Tuple[int, int, int]):
        self._rgb_color = rgb
        packet = bytearray([0x55, 0x07, 0x01, rgb[0], rgb[1], rgb[2]])
        await self._write(packet)
        self._effect = EFFECT_OFF

    @retry_bluetooth_connection_error
    async def set_brightness(self, brightness: int):
        self._brightness = brightness
        b = min(int(brightness * 0.06), 0x0f)
        packet = bytearray([0x55, 0x03, 0x01, 0xff, b])
        await self._write(packet)

    @retry_bluetooth_connection_error
    async def set_effect(self, effect: str):
        if effect not in EFFECT_MAP:
            LOGGER.error("Unsupported effect: %s", effect)
            return
        effect_id = EFFECT_MAP[effect]
        packet = bytearray([0x55, 0x04, 0x01, effect_id])
        await self._write(packet)
        self._effect = effect

    @retry_bluetooth_connection_error
    async def set_effect_speed(self, speed: int):
        speed_byte = min(max(int(speed * 2.55), 0), 255)
        packet = bytearray([0x55, 0x04, 0x04, speed_byte])
        await self._write(packet)

    async def _write(self, data: bytearray):
        await self._ensure_connected()
        await self._client.write_gatt_char(self._write_uuid, data, False)

    async def _ensure_connected(self):
        if self._client and self._client.is_connected:
            self._reset_disconnect_timer()
            return

        async with self._connect_lock:
            if self._client and self._client.is_connected:
                self._reset_disconnect_timer()
                return

            LOGGER.debug("%s: Connecting...", self.name)
            client = await establish_connection(
                BleakClientWithServiceCache,
                self._device,
                self.name,
                self._disconnected,
                use_cached_services=True,
                ble_device_callback=lambda: self._device,
            )

            if not self._resolve_characteristics(client.services):
                LOGGER.debug("Initial resolve failed, trying with fresh services")
                await client.get_services()
                self._resolve_characteristics(client.services)

            self._client = client
            self._cached_services = client.services
            self._reset_disconnect_timer()

    def _resolve_characteristics(self, services: BleakGATTServiceCollection) -> bool:
        self._write_uuid = services.get_characteristic(WRITE_CHARACTERISTIC_UUID)
        self._manufacturer_name_char = services.get_characteristic(MANUFACTURER_NAME_UUID)
        self._firmware_revision_char = services.get_characteristic(FIRMWARE_REVISION_UUID)
        self._model_number_char = services.get_characteristic(SW_NUMBER_UUID)
        return all([self._write_uuid, self._manufacturer_name_char, self._firmware_revision_char, self._model_number_char])

    def _reset_disconnect_timer(self):
        if self._disconnect_timer:
            self._disconnect_timer.cancel()
        self._expected_disconnect = False
        if self._delay:
            self._disconnect_timer = self.loop.call_later(self._delay, self._disconnect)

    def _disconnected(self, client):
        if self._expected_disconnect:
            LOGGER.debug("%s: Disconnected (expected)", self.name)
        else:
            LOGGER.warning("%s: Unexpected disconnect", self.name)

    def _disconnect(self):
        self._disconnect_timer = None
        asyncio.create_task(self._execute_disconnect())

    async def _execute_disconnect(self):
        async with self._connect_lock:
            if self._client and self._client.is_connected:
                self._expected_disconnect = True
                await self._client.disconnect()
                LOGGER.debug("%s: Disconnected", self.name)
            self._client = None
            self._write_uuid = None
