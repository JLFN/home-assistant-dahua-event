"""
Attach event listener to Dahua devices
Borrowed code from https://github.com/johnnyletrois/dahua-watch

Author: Akram
"""

import threading, logging, os, socket, requests, time
import voluptuous as vol
import homeassistant.helpers.config_validation as cv
from requests.auth import HTTPDigestAuth
from homeassistant.const import (
    EVENT_HOMEASSISTANT_START,
    EVENT_HOMEASSISTANT_STOP,
    CONF_NAME)

_LOGGER = logging.getLogger(__name__)
DOMAIN = 'dahua_event'

URL_TEMPLATE = "{protocol}://{host}:{port}/cgi-bin/eventManager.cgi?action=attach&channel={channel}&codes=%5B{events}%5D"

CONFIG_SCHEMA = vol.Schema({
    DOMAIN:
        vol.All(cv.ensure_list, [vol.Schema({
            vol.Optional(CONF_NAME): cv.string,
            vol.Optional("protocol", default="http"): cv.string,
            vol.Optional("user", default="admin"): cv.string,
            vol.Optional("password", default="admin"): cv.string,
            vol.Required("host"): cv.string,
            vol.Optional("port", default=80): int,
            vol.Optional("channel", default=1): int,
            vol.Optional("events", default="VideoMotion,CrossLineDetection,AlarmLocal,VideoLoss,VideoBlind"): cv.string,
        })])
}, extra=vol.ALLOW_EXTRA)


def setup(hass, config):
    """Set up Dahua event listener."""
    config = config.get(DOMAIN)

    dahua_event = DahuaEventThread(
        hass,
        config
    )

    def _start_dahua_event(_event):
        dahua_event.start()

    def _stop_dahua_event(_event):
        dahua_event.stopped.set()

    hass.bus.listen_once(
        EVENT_HOMEASSISTANT_START,
        _start_dahua_event
    )
    hass.bus.listen_once(
        EVENT_HOMEASSISTANT_STOP,
        _stop_dahua_event
    )

    return True


class DahuaDevice():
    def __init__(self, hass, master, name, url, channel):
        self.hass = hass
        self.Master = master
        self.Name = name
        self.Url = url
        self.Channels = channel
        self.RequestObj = None
        self.Connected = None
        self.Reconnect = None

    def OnConnect(self):
        _LOGGER.debug("[{0}] OnConnect()".format(self.Name))
        self.Connected = True

    def OnDisconnect(self, reason):
        _LOGGER.debug("[{0}] OnDisconnect({1})".format(self.Name, reason))
        self.Connected = False


    def OnReceive(self, data):
        Data = data.decode("utf-8", errors="ignore")
        _LOGGER.debug("[{0}]: {1}".format(self.Name, Data))

        for Line in Data.split("\r\n"):
            if Line == "HTTP/1.1 200 OK":
                self.OnConnect()

            if not Line.startswith("Code="):
                continue

            Alarm = dict()
            Alarm["name"] = self.Name
            for KeyValue in Line.split(';'):
                Key, Value = KeyValue.split('=')
                Alarm[Key] = Value

            if Alarm["index"] in self.Channels:
                Alarm["channel"] = self.Channels[Alarm["index"]]

            self.hass.bus.fire("dahua_event_received", Alarm)

class DahuaEventThread(threading.Thread):
    """Connects to device and subscribes to events"""
    Devices = []
    NumActivePlayers = 0

    # CurlMultiObj = pycurl.CurlMulti()
    NumRequestObjs = 0
	

    def __init__(self, hass, config):
        """Construct a thread listening for events."""
        self.hass = hass

        for device_cfg in config:
          url = URL_TEMPLATE.format(
              protocol=device_cfg.get("protocol"),
              host=device_cfg.get("host"),
              port=device_cfg.get("port"),
              channel=device_cfg.get("channel"),
              events=device_cfg.get("events")
            )
          device = DahuaDevice(self, hass, device_cfg.get("name"), url, channel)
          auth = HTTPDigestAuth(device_cfg.get("user"), device_cfg.get("password"))
          self.Devices.append(device)
	  RequestObj = requests.get(url, hooks=dict(args=device.OnReceive), stream=True, timeout=30,auth=auth)
          device.RequestObj = RequestObj
          # RequestObj.setopt(pycurl.WRITEFUNCTION, device.OnReceive)

          # self.CurlMultiObj.add_handle.RequestObj)
          # self.NumRequestObjs += 1

          _LOGGER.debug("Added Dahua device at: %s", url)

        threading.Thread.__init__(self)
        self.stopped = threading.Event()

    def run(self):
        """Fetch events"""
        while 1:
            Ret, NumHandles = self.CurlMultiObj.perform()
            if Ret != pycurl.E_CALL_MULTI_PERFORM:
                break

        Ret = self.CurlMultiObj.select(1.0)
        while not self.stopped.isSet():
            # Sleeps to ease load on processor
            time.sleep(.05)
            Ret, NumHandles = self.CurlMultiObj.perform()

            if NumHandles != self.NumRequestObjs:
                _, Success, Error = self.CurlMultiObj.info_read()

                for RequestObj in Success:
                    DahuaDevice = next(filter(lambda x: x.RequestObj == RequestObj, self.Devices))
                    if DahuaDevice.Reconnect:
                        continue

                    DahuaDevice.OnDisconnect("Success")
                    DahuaDevice.Reconnect = time.time() + 5

                for RequestObj, ErrorNo, ErrorStr in Error:
                    DahuaDevice = next(filter(lambda x: x.RequestObj == RequestObj, self.Devices))
                    if DahuaDevice.Reconnect:
                        continue

                    DahuaDevice.OnDisconnect("{0} ({1})".format(ErrorStr, ErrorNo))
                    DahuaDevice.Reconnect = time.time() + 5

                for DahuaDevice in self.Devices:
                    if DahuaDevice.Reconnect and DahuaDevice.Reconnect < time.time():
                        self.CurlMultiObj.remove_handle(DahuaDevice.RequestObj)
                        self.CurlMultiObj.add_handle(DahuaDevice.RequestObj)
                        DahuaDevice.Reconnect = None
            #if Ret != pycurl.E_CALL_MULTI_PERFORM: break


