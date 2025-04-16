#!/usr/bin/env python3
"""
High‑speed reader for BOTA force/torque sensors
   • Logs to CSV with wall‑clock derived from Sensor_Timestamp (µs)
   • Zero host‑side timestamp jitter
"""

import sys
import struct
import time
import threading
import csv
from datetime import datetime, timedelta
from collections import namedtuple

import pytz
import serial
from crc import Calculator, Configuration


class BotaSerialSensorError(Exception):
    """Custom exception for serial‑related problems"""
    pass


class BotaSerialSensor:
    # ---------------------------- device constants ---------------------------
    BOTA_PRODUCT_CODE  = 123456
    BAUDRATE           = 460_800
    SINC_LENGTH        = 64
    CHOP_ENABLE        = 0
    FAST_ENABLE        = 1
    FIR_DISABLE        = 1
    TEMP_COMPENSATION  = 0   # 0: Disabled (recommended), 1: Enabled
    USE_CALIBRATION    = 1   # 1: calibration matrix active, 0: raw
    DATA_FORMAT        = 0   # 0: binary, 1: CSV
    BAUDRATE_CONFIG    = 4   # 0:9600 1:57600 2:115200 3:230400 4:460800
    FRAME_HEADER       = b'\xAA'

    # ------------------------------------------------------------------------
    def __init__(self, port: str) -> None:
        self._port                = port
        self._ser                 = serial.Serial()
        self._pd_thread_stop      = threading.Event()

        DeviceSet = namedtuple('DeviceSet', 'name product_code config_func')
        self._expected_device     = {
            0: DeviceSet('BFT-SENS-SER-M8',
                         self.BOTA_PRODUCT_CODE,
                         self._sensor_setup)
        }

        # ─── runtime data fields ────────────────────────────────────────────
        self.tz_chicago = pytz.timezone('America/Chicago')

        self._status = 0
        self._fx = self._fy = self._fz = 0.0
        self._mx = self._my = self._mz = 0.0
        self._timestamp = 0               # raw µs from sensor (32‑bit)
        self._temperature = 0.0

        # csv
        self._csvfile   = None
        self._csvwriter = None

        # timestamp helpers
        self._start_time                = None  # wall clock at first frame
        self._initial_sensor_timestamp  = None  # first µs counter value
        self._prev_sensor_timestamp     = None  # to detect rollovers
        self._rollover_count            = 0     # 32‑bit wraps

    # ------------------------------------------------------------------------
    #                          sensor configuration
    # ------------------------------------------------------------------------
    def _sensor_setup(self) -> bool:
        """Put sensor in RUN mode with desired filter & baud settings"""
        print("Configuring sensor …")

        # Wait for initial streaming text ("App Init")
        if not self._wait_for(b'App Init'):
            print("Sensor not streaming – wrong port?")
            return False

        self._ser.reset_input_buffer()
        self._ser.reset_output_buffer()

        if not self._command(b'C', b'r,0,C,0'):   # CONFIG mode
            print("Failed to enter CONFIG mode.")
            return False

        comm = f"c,{self.TEMP_COMPENSATION},{self.USE_CALIBRATION}," \
               f"{self.DATA_FORMAT},{self.BAUDRATE_CONFIG}"
        if not self._command(comm.encode(), b'r,0,c,0'):
            print("Communication setup failed.")
            return False

        # Sample period depends on sinc length (not used for ts math anymore)
        self.time_step = 0.00001953125 * self.SINC_LENGTH
        print(f"Timestep (for reference): {self.time_step:.8f} s")

        filt = f"f,{self.SINC_LENGTH},{self.CHOP_ENABLE}," \
               f"{self.FAST_ENABLE},{self.FIR_DISABLE}"
        if not self._command(filt.encode(), b'r,0,f,0'):
            print("Filter setup failed.")
            return False

        if not self._command(b'R', b'r,0,R,0'):   # RUN
            print("Failed to enter RUN mode.")
            return False

        return True

    # ------------------------------------------------------------------------
    #                           serial helpers
    # ------------------------------------------------------------------------
    def _wait_for(self, token: bytes, timeout: float = 10.0) -> bool:
        """Read until token or timeout"""
        self._ser.timeout = timeout
        data = self._ser.read_until(token)
        return token in data

    def _command(self, cmd: bytes, expect: bytes) -> bool:
        """Send cmd, wait for expect, return success"""
        self._ser.write(cmd)
        return self._wait_for(expect)

    # ------------------------------------------------------------------------
    #                   high‑rate binary frame processing
    # ------------------------------------------------------------------------
    def _processdata_thread(self) -> None:
        """Continuously read frames, verify CRC, write CSV rows"""
        crc_cfg   = Configuration(16, 0x1021, 0xFFFF, 0xFFFF, True, True)
        crc_calc  = Calculator(crc_cfg)

        header = self.FRAME_HEADER

        while not self._pd_thread_stop.is_set():
            # ─── sync to header byte ────────────────────────────────────
            if self._ser.read(1) != header:
                continue
            data_frame = self._ser.read(34)
            crc_recv   = struct.unpack_from('H', self._ser.read(2))[0]
            if crc_calc.checksum(data_frame) != crc_recv:
                continue  # bad frame – resync

            # ─── parse binary frame ─────────────────────────────────────
            self._status      = struct.unpack_from('H', data_frame, 0)[0]
            self._fx          = struct.unpack_from('f', data_frame, 2)[0]
            self._fy          = struct.unpack_from('f', data_frame, 6)[0]
            self._fz          = struct.unpack_from('f', data_frame, 10)[0]
            self._mx          = struct.unpack_from('f', data_frame, 14)[0]
            self._my          = struct.unpack_from('f', data_frame, 18)[0]
            self._mz          = struct.unpack_from('f', data_frame, 22)[0]
            self._timestamp   = struct.unpack_from('I', data_frame, 26)[0]  # µs
            self._temperature = struct.unpack_from('f', data_frame, 30)[0]

            # ─── establish t0 on very first good frame ──────────────────
            if self._initial_sensor_timestamp is None:
                self._initial_sensor_timestamp = self._timestamp
                self._prev_sensor_timestamp    = self._timestamp
                self._start_time = datetime.now(self.tz_chicago)
            else:
                # handle 32‑bit rollover (wrap every ~71 min)
                if self._timestamp < self._prev_sensor_timestamp:
                    self._rollover_count += 1
                self._prev_sensor_timestamp = self._timestamp

            # 64‑bit extended microsecond counter
            full_us = (self._rollover_count << 32) + self._timestamp
            init_us = (self._rollover_count << 32) + self._initial_sensor_timestamp
            delta_us = full_us - init_us

            sample_time = self._start_time + timedelta(microseconds=delta_us)

            # ─── CSV row ────────────────────────────────────────────────
            self._csvwriter.writerow([
                sample_time.isoformat(),  # America/Chicago
                self._status,
                self._fx, self._fy, self._fz,
                self._mx, self._my, self._mz,
                self._timestamp,          # raw 32‑bit µs counter
                self._temperature
            ])
            self._csvfile.flush()  # or use buffering if perf demands

    # ------------------------------------------------------------------------
    #                   optional slow diagnostic print loop
    # ------------------------------------------------------------------------
    def _telemetry_loop(self) -> None:
        try:
            while True:
                print(f"Fx:{self._fx:+.3f}  Fy:{self._fy:+.3f}  Fz:{self._fz:+.3f}  "
                      f"Mx:{self._mx:+.3f}  My:{self._my:+.3f}  Mz:{self._mz:+.3f}  "
                      f"Ts:{self._timestamp}  T:{self._temperature:.2f}")
                time.sleep(1.0)
        except KeyboardInterrupt:
            print("User interrupted telemetry loop.")

    # ------------------------------------------------------------------------
    #                                 run
    # ------------------------------------------------------------------------
    def run(self, csv_prefix: str = 'sensor_data') -> None:
        # serial port setup
        self._ser.baudrate = self.BAUDRATE
        self._ser.port     = self._port
        self._ser.timeout  = 10

        # CSV file prep
        ts_str      = datetime.now().strftime('%Y%m%d_%H%M%S')
        csv_name    = f"{csv_prefix}_{ts_str}.csv"
        self._csvfile   = open(csv_name, 'w', newline='')
        self._csvwriter = csv.writer(self._csvfile)
        self._csvwriter.writerow([
            'Chicago_Time', 'Status',
            'Fx', 'Fy', 'Fz', 'Mx', 'My', 'Mz',
            'Sensor_Timestamp_µs', 'Temperature'
        ])

        # open port
        try:
            self._ser.open()
            print(f"Serial port {self._port} opened.")
        except Exception as exc:
            raise BotaSerialSensorError(f"Could not open {self._port}: {exc}")

        # sensor configuration
        if not self._sensor_setup():
            self._csvfile.close()
            raise BotaSerialSensorError("Sensor setup failed.")

        # start frame‑processing thread
        proc = threading.Thread(target=self._processdata_thread, daemon=True)
        proc.start()

        # optional live printout
        self._telemetry_loop()

        # cleanup on exit
        self._pd_thread_stop.set()
        proc.join()
        self._ser.close()
        self._csvfile.close()


# --------------------------------------------------------------------------- #
#                                   main
# --------------------------------------------------------------------------- #
if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: bota_serial_example <portname> [csv_prefix]")
        sys.exit(1)

    portname = sys.argv[1]
    prefix   = sys.argv[2] if len(sys.argv) > 2 else 'sensor_data'

    try:
        sensor = BotaSerialSensor(portname)
        sensor.run(csv_prefix=prefix)
    except BotaSerialSensorError as e:
        print(f"Fatal: {e}")
        sys.exit(1)
