# Flux Capturer

Flux Capturer is the transport/capture layer used by KTY Labs applications to decouple serial/TCP I/O from the UI process. This repository contains the host process and the .NET client library.

## Projects

- `FluxCapturer` — background host that owns transports and publishes captured data.
- `FluxCapturer.Client` — .NET client library used by applications to start/connect to the host, open serial or TCP transports, transmit data, and receive captured data.

## Build

Requirements: .NET 8 SDK on Windows.

```text
dotnet build FluxCapturer.sln -c Release
```

## Run

Normal KTY application-managed host mode:

```text
FluxCapturer.exe
```

Standalone/open-source host mode (does not require Serial Flux or CAN Coyote X7 to remain running):

```text
FluxCapturer.exe --standalone
```

Legacy direct serial capture mode:

```text
FluxCapturer.exe COM5 115200
```

## Use from another .NET application

Reference `FluxCapturer.Client`. The current public client surfaces are `FluxCapturerProcessManager`, `FluxCapturerClient`, and `FluxTransportSession`.

`FluxTransportSession` supports serial and TCP transport sessions and exposes received bytes through `DataReceived`.

## Relationship to Serial Flux

Flux Capturer is a separate project/repository. Serial Flux can reference a sibling clone during development and bundle the resulting Flux Capturer host/client binaries in its own installer. End users do not need a separate Flux Capturer installer to run Serial Flux.

## Protocol

See `docs/PROTOCOL.md` for the framing behavior implemented by the current source. Flux Capturer deliberately transports complete STM32 frames without interpreting application-level CAN command IDs.

## License

MIT. See `LICENSE`.
