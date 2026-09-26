# Flux Capturer .NET API

The repository currently exposes two client paths.

## FluxTransportSession

Use `FluxTransportSession` for application-managed serial/TCP transport I/O through the Flux Capturer host.

Key members currently implemented:

- `OpenSerialAsync(...)`
- `OpenTcpAsync(...)`
- `SendAsync(byte[])`
- `IsConnected`
- `DataReceived`
- `Faulted`

## FluxCapturerProcessManager

Use `FluxCapturerProcessManager.EnsureHostRunningAsync()` when an application should ensure the bundled host is running before opening a transport.

The manager locates `FluxCapturer.exe`, starts it when necessary, and waits for the host IPC endpoint to become available.

## FluxCapturerClient

`FluxCapturerClient` is the shared-memory capture client. It exposes availability/capture access used by existing KTY applications.

## API stability

Version 1.0 initially preserves the existing API used by Serial Flux. Higher-level CAN-specific wrappers, Python bindings, and python-can integration should be layered on top rather than changing the v1 transport contract.
