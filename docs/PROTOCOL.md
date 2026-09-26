# Flux Capturer Protocol Notes

This document describes what the current Flux Capturer source actually implements.

## STM32 serial framing

The capture server recognizes complete STM32 frames using this envelope:

| Offset | Size | Description |
|---:|---:|---|
| 0 | 1 | Preamble `0x55` |
| 1 | 1 | Preamble `0xAA` |
| 2 | 2 | Payload size, little-endian |
| 4 | N | Payload (`ID + data`) |
| 4+N | 1 | Checksum |

Flux Capturer currently separates complete frames and forwards them. It does **not** interpret the command ID, CAN ID, data, or checksum semantics; that remains the responsibility of the consuming application/protocol layer.

Invalid bytes before `55 AA` are skipped. A payload size below 1 is treated as invalid and scanning resumes at the next possible preamble.

## Host IPC

The current Windows host uses:

- Shared memory name: `Local\\KTYLabs_FluxCapturer_Shared_v1`
- Named pipe name: `KTYLabs_FluxCapturer_Transport_v1`
- Single-instance mutex: `Local\\KTYLabs_FluxCapturer_SingleInstance_v1`

These names are part of the current v1 compatibility surface.

## Runtime transport frames

The client/host named-pipe runtime uses three frame types:

- `1` — TX data
- `2` — RX data
- `3` — TX acknowledgement

A runtime frame contains a one-byte type, an unsigned 64-bit sequence number, then payload bytes. Pipe messages themselves are prefixed with a 32-bit payload length by the current implementation.

## Versioning

Changes that break shared-memory layout, pipe framing, or the names above should be treated as a protocol compatibility change rather than an ordinary implementation change.
