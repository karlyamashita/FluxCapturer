# Flux Capturer examples

## Interactive serial example

```text
dotnet run --project FluxCapturer.Examples -- COM5 115200
```

The example ensures the FC host is running, opens the requested serial port,
prints received bytes, accepts hexadecimal TX bytes from the console, and
disposes the transport session cleanly.

Example input:

```text
55 AA 01 00 90 90
```

See `../../docs/API.md` for additional examples covering TCP, shared-memory
capture, statistics, process leases, error handling, and STM32 framing.
