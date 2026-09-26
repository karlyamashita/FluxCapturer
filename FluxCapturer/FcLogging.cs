namespace FluxCapturer;

internal enum FcLogLevel { Off = 0, Error = 1, Warning = 2, Info = 3, Debug = 4 }

internal static class FcLogging
{
    internal static bool Enabled { get; private set; }
    internal static FcLogLevel Level { get; private set; } = FcLogLevel.Debug;
    internal static bool LogToFile { get; private set; } = true;
    internal static bool LogToConsole { get; private set; }

    internal static bool Allows(FcLogLevel level) => Enabled && level != FcLogLevel.Off && Level >= level;

    // Load once at startup. Missing INI means diagnostics remain disabled.
    internal static void Initialize(string[] args)
    {
        string path = Path.Combine(AppContext.BaseDirectory, "FluxCapturer.ini");
        if (File.Exists(path))
        {
            try
            {
                bool loggingSection = false;
                foreach (string raw in File.ReadLines(path))
                {
                    string line = raw.Trim();
                    if (line.Length == 0 || line.StartsWith(';') || line.StartsWith('#')) continue;
                    if (line.StartsWith('[') && line.EndsWith(']'))
                    {
                        loggingSection = line.Equals("[Logging]", StringComparison.OrdinalIgnoreCase);
                        continue;
                    }
                    if (!loggingSection) continue;
                    int equals = line.IndexOf('=');
                    if (equals < 0) continue;
                    string key = line[..equals].Trim();
                    string value = line[(equals + 1)..].Trim();
                    if (key.Equals("Enabled", StringComparison.OrdinalIgnoreCase) && bool.TryParse(value, out bool enabled)) Enabled = enabled;
                    else if (key.Equals("Level", StringComparison.OrdinalIgnoreCase) && Enum.TryParse(value, true, out FcLogLevel level)) Level = level;
                    else if (key.Equals("LogToFile", StringComparison.OrdinalIgnoreCase) && bool.TryParse(value, out bool file)) LogToFile = file;
                    else if (key.Equals("LogToConsole", StringComparison.OrdinalIgnoreCase) && bool.TryParse(value, out bool console)) LogToConsole = console;
                }
            }
            catch (Exception ex)
            {
                Enabled = false;
                Console.Error.WriteLine($"Flux Capturer logging configuration error: {ex.Message}");
            }
        }
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i].Equals("--log-level", StringComparison.OrdinalIgnoreCase) && i + 1 < args.Length &&
                Enum.TryParse(args[i + 1], true, out FcLogLevel level))
            {
                Level = level;
                Enabled = level != FcLogLevel.Off;
                i++;
            }
        }
    }
}
