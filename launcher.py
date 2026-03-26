"""
    ASRS Project Launcher
    ==================================================================================================
    Starts server.py and mqtt.py as independent subprocesses.
    Press Ctrl+C to stop both cleanly.
    ==================================================================================================
"""

import subprocess
import sys
import threading
import time
import os
import signal

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PYTHON   = sys.executable


def stream_output(proc: subprocess.Popen, prefix: str):
    """Read stdout/stderr from a process and print with a label prefix."""
    for line in iter(proc.stdout.readline, b""):
        print(f"[{prefix}] {line.decode(errors='replace').rstrip()}", flush=True)


def start_process(script: str, label: str) -> subprocess.Popen:
    path = os.path.join(BASE_DIR, script)
    proc = subprocess.Popen(
        [PYTHON, path],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=BASE_DIR,
    )
    thread = threading.Thread(target=stream_output, args=(proc, label), daemon=True)
    thread.start()
    print(f"[launcher] Started {label} (PID {proc.pid})")
    return proc


def stop_process(proc: subprocess.Popen, label: str):
    if proc.poll() is None:
        print(f"[launcher] Stopping {label} (PID {proc.pid})...")
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        print(f"[launcher] {label} stopped.")


def main():
    print("[launcher] Starting ASRS project...")
    print("[launcher] ─────────────────────────────────────────")

    # Start server first so SocketIO is ready before MQTT publishes
    server = start_process("server.py", "SERVER")
    time.sleep(1.5)   # give Flask/SocketIO a moment to bind

    mqtt = start_process("mqtt.py", "MQTT")

    print("[launcher] ─────────────────────────────────────────")
    print("[launcher] All processes running. Press Ctrl+C to stop.")
    print("[launcher] Dashboard → http://127.0.0.1:5001")
    print("[launcher] ─────────────────────────────────────────")

    try:
        while True:
            # Exit if either process dies unexpectedly
            if server.poll() is not None:
                print(f"[launcher] SERVER exited with code {server.returncode}. Shutting down.")
                stop_process(mqtt, "MQTT")
                break
            if mqtt.poll() is not None:
                print(f"[launcher] MQTT exited with code {mqtt.returncode}. Shutting down.")
                stop_process(server, "SERVER")
                break
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[launcher] Ctrl+C received. Shutting down...")
        stop_process(mqtt,   "MQTT")
        stop_process(server, "SERVER")

    print("[launcher] All processes stopped. Goodbye.")


if __name__ == "__main__":
    main()
