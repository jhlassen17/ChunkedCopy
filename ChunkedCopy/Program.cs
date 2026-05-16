using Microsoft.Win32.SafeHandles;
using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Drawing;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

partial

/// <summary>
/// Provides the entry point for the Chunked File Copy application, enabling parallel and resumable file or directory
/// copy operations with optional verification.
/// </summary>
/// <remarks>This class is intended to be run as a console application. It parses command-line arguments to
/// configure copy behavior, including source and destination paths, buffer size, parallelism, resume support, and
/// verification. The application supports both single file and directory copy modes, and provides progress reporting
/// and optional hash verification for data integrity. For usage details, run the application with no arguments to use
/// default values, or supply command-line options as needed.</remarks>
class Program
{
    /// <summary>
    /// Main Task
    /// </summary>
    /// <param name="args">CLI Args</param>
    /// <returns>Task</returns>
    static async Task Main(string[] args)
    {
        // Set up vars
        string? source = null;
        string? dest = null;
        //int bufferMB = 32;
        int bufferMB = 64;
        int parallel = 2;
        bool resume = true;
        bool verify = false;
        bool isDirectory = false;
        string? filter = null;

        // Set up console
        Console.Title = "Chunked Copy (Global Scheduler)";
        Console.OutputEncoding = Encoding.UTF8;

        // CLI Args
        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--dir":
                    isDirectory = true;
                    break;
                case "-s":
                case "--source":
                    source = args[++i];
                    break;
                case "-d":
                case "--dest":
                    dest = args[++i];
                    break;
                case "-b":
                case "--buffer":
                    bufferMB = int.Parse(args[++i]);
                    break;
                case "-p":
                case "--parallel":
                    parallel = int.Parse(args[++i]);
                    break;
                case "--no-resume":
                    resume = false;
                    break;
                case "--verify":
                    verify = true;
                    break;
                case "--filter":
                    filter = args[++i];
                    break;
            }
        }

        // Fallback to our hardcoded values if not provided
        source ??= @"J:\jeff\files\Travel\Movies";  // @"J:\jeff\files\3D\Movies"; // @"H:\jeff\files\temp\ISOs\";
        dest ??= @"M:\Movies";   // @"J:\jeff\files\Video\TV\Curb Your Enthusiasm (2000) {tvdb-76203}";
        if (Directory.Exists(source) && Debugger.IsAttached) isDirectory = true;

        // Make sure that we got both source and destination paths
        if (string.IsNullOrWhiteSpace(source) || string.IsNullOrWhiteSpace(dest))
        {
            Console.WriteLine("Source and destination paths must be specified.");
            return;
        }

        // Check to see if the source exists and is accessible
        if (isDirectory && !Directory.Exists(source))
        {
            Console.WriteLine("Could not access source directory path.");
            return;
        }
        else if (!isDirectory && !File.Exists(source))
        {
            Console.WriteLine("Could not access source file path.");
            return;
        }
        else
        {
            // Good to go
            Console.WriteLine($"Processing source path: {Path.GetFileName(source)}");
        }

        // Check destination path - if it's a directory copy, we want to create the directory if it doesn't exist.
        // If it's a file copy, we want to make sure the destination file doesn't already exist and that the destination
        // directory does exist (or can be created)
        if (isDirectory && !Directory.Exists(dest))
        {
            Directory.CreateDirectory(dest);
        }
        else if (!isDirectory && File.Exists(dest) && !resume)
        {
            Console.WriteLine("Destination file already exists and resume is not set.");
            return;
        }
        else if (!isDirectory && !Directory.Exists(Path.GetDirectoryName(dest)))
        {
            // Try to make the parent directory if it doesn't exist
            Console.WriteLine("Could not access destination directory path...creating");
            Directory.CreateDirectory(Path.GetDirectoryName(dest)!);
        }

        // Set chunk size in bytes
        int chunkSize = bufferMB * 1024 * 1024;

        // Alert
        Console.WriteLine($"Parallel: {parallel} | Chunk: {bufferMB}MB | Resume: {resume} | Verify: {verify}");

        // Check directory flag and copy accordingly
        if (isDirectory)
        {
            if (!String.IsNullOrWhiteSpace(filter))
            {
                await CopyDirectoryGlobal(source, dest, chunkSize, parallel, resume, verify, filter);
            }
            else
            {
                await CopyDirectoryGlobal(source, dest, chunkSize, parallel, resume, verify);
            }
        }
        else
        {
            if (!String.IsNullOrWhiteSpace(filter))
            {
                await CopyFileGlobal(source, dest, chunkSize, parallel, resume, verify, filter);
            }
            else
            {
                await CopyFileGlobal(source,dest,chunkSize,parallel, resume, verify);
            }

            // Check to see if we should verify the copy by comparing hashes of the source and destination files
            if (verify)
            {
                Console.WriteLine("\nVerifying...");
                Console.WriteLine(await HashFile(source) == await HashFile(dest)
                    ? "✅ VERIFIED"
                    : "❌ MISMATCH");
            }
        }

        // Done
        Console.WriteLine("\nDone.");
    }

    // =========================
    // GLOBAL DIRECTORY COPY
    // =========================
    static async Task CopyDirectoryGlobal(string sourceDir, string destDir, int chunkSize, int parallel, bool resume, bool verify, string filter = "*")
    {
        string[] files;

        Console.WriteLine($"Skipping non-\"{filter}\" file");
        files = Directory.GetFiles(sourceDir, filter, SearchOption.AllDirectories);


        // Calculate total bytes for progress reporting
        long totalBytes = 0;
        foreach (var f in files)
            totalBytes += new FileInfo(f).Length;

        // Set up counters
        //long processed = 0;
        long totalWritten = 0;
        int index = 0;

        // Update console with total files and size
        Console.WriteLine($"Files: {files.Length} | Total: {Fmt(totalBytes)}\n");

        // Start stopwatch
        var sw = Stopwatch.StartNew();

        // Set up queues and dictionaries
        var stateWriters = new ConcurrentDictionary<string, StateWriter>();
        var completedMaps = new ConcurrentDictionary<string, ConcurrentDictionary<int, bool>>();
        //var dstHandles = new ConcurrentDictionary<string, Lazy<SafeFileHandle>>();
        //var srcHandles = new ConcurrentDictionary<string, Lazy<SafeFileHandle>>();
        //var initializedFiles = new ConcurrentDictionary<string, boWl>();
        // ✅ NEW: file metadata (fixes resume + avoids dst dependency)
        var fileMeta = new ConcurrentDictionary<string, (string src, int chunkCount, ConcurrentDictionary<int, bool> completed)>();
        var maxWrittenOffsets = new ConcurrentDictionary<string, long>();

        // ✅ Build global chunk queue
        foreach (var src in files)
        {
            var chunkQueue = new ConcurrentQueue<(string src, string dst, long offset, int size, int index)>();
            var fileWriteLocks = new ConcurrentDictionary<string, SemaphoreSlim>();

            // Count
            index++;

            // Set up paths
            string rel = Path.GetRelativePath(sourceDir, src);
            string dst = Path.Combine(destDir, rel);

            // Creeate destination directory if it doesn't exist
            Directory.CreateDirectory(Path.GetDirectoryName(dst)!);

            // Update console with current file being processed
            Console.WriteLine($"\n[{index}/{files.Length}] {rel}");

            //  Calculate sizes, chunks, and state
            long fileSize = new FileInfo(src).Length;
            //InitializeDestinationFile(dst, fileSize);
            if (NormalizeTitle(src) == NormalizeTitle(dst))
            {
                if (File.Exists(dst) && (new FileInfo(dst).Length == fileSize))
                {
                    continue;
                }
            }

            // 
            if (!File.Exists(dst))
            {
                using var fs = new FileStream(dst, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite);
            }

            //// ***** Filter *****
            //if(!src.Contains("[3D HSBS]"))
            //{
            //    Console.WriteLine("Skipping non-3D file");
            //    continue;
            //}

            // ✅ SMALL FILE FAST PATH (skip chunking)
            if (fileSize <= chunkSize)
            {
                if (resume && File.Exists(dst))
                {
                    if (new FileInfo(dst).Length == fileSize)
                    {
                        Interlocked.Add(ref totalWritten, fileSize);
                        RenderProgress(totalWritten, totalBytes, sw.Elapsed);
                        continue;
                    }
                }

                if (!File.Exists(dst) || new FileInfo(dst).Length != fileSize)
                {
                    File.Copy(src, dst, true);
                }

                Interlocked.Add(ref totalWritten, fileSize);
                RenderProgress(totalWritten, totalBytes, sw.Elapsed);

                continue;
            }

            // Calculate chunk count and set up completed dictionary for this file
            int chunkCount = (int)Math.Ceiling((double)fileSize / chunkSize);
            var completed = new ConcurrentDictionary<int, bool>();
            string stateFile = dst + ".state.json";
            Debug.Print($"***dst: \"{dst}\" - stateFile: \"{stateFile}\"");

            // Check to see if we are resuming
            if (resume && File.Exists(stateFile))
            {
                try
                {
                    // Load the JSON and copy the details into the array of completed chunks
                    int[] saved = [];

                    for (int attempt = 0; attempt < 3; attempt++)
                    {
                        try
                        {
                            var json = File.ReadAllText(stateFile);
                            saved = JsonSerializer.Deserialize<int[]>(json) ?? [];
                            break;
                        }
                        catch
                        {
                            Thread.Sleep(100);
                        }
                    }

                    foreach (var i in saved)
                        completed[i] = true;
                }
                catch { /* Silent Ignore */ }
            }

            // ✅ OPTIONAL IMPROVEMENT (initialize maxWrittenOffsets from state)
            if (! completed.IsEmpty)
            {
                long max = (long)(completed.Keys.Max() + 1) * chunkSize;

                maxWrittenOffsets[dst] = max;
            }
            else
            {
                maxWrittenOffsets.TryAdd(dst, 0);
            }

            //else if (File.Exists(dst))
            //{
            //    // ✅ assume full file if no state file
            //    long fileSize2 = new FileInfo(src).Length;
            //    int chunkCount2 = (int)Math.Ceiling((double)fileSize2 / chunkSize);

            //    for (int i = 0; i < chunkCount2; i++)
            //        completed[i] = true;
            //}

            // Set up state
            completedMaps[dst] = completed;
            stateWriters[dst] = new StateWriter(stateFile, completed);
            fileMeta[dst] = (src, chunkCount, completed);
            //maxWrittenOffsets.TryAdd(dst, 0);

            // Loop through each chunk and process in parallel
            for (int i = 0; i < chunkCount; i++)
            {
                long offset = (long)i * chunkSize;
                int size = (int)Math.Min(chunkSize, fileSize - offset);

                bool alreadyDone = completed.ContainsKey(i);

                if (alreadyDone)
                    continue;

                chunkQueue.Enqueue((src, dst, offset, size, i));

                //if (alreadyDone) continue;

                //chunkQueue.Enqueue((src, dst, offset, size, i));
            }

            // Set up workers
            var workers = new List<Task>();

            for (int w = 0; w < parallel; w++)
            {
                workers.Add(Task.Run(async () =>
                {
                    byte[] buffer = new byte[chunkSize];

                    SafeFileHandle? srcHandle = null;
                    SafeFileHandle? dstHandle = null;
                    string? currentSrc = null;
                    string? currentDst = null;

                    while (chunkQueue.TryDequeue(out var job))
                    {
                        // ✅ EXPLICIT TYPE (optional but safer)
                        (string src2, string dst2, long offset, int size, int index2) = job;

                        if (srcHandle == null || currentSrc != src2)
                        {
                            srcHandle?.Dispose();
                            srcHandle = File.OpenHandle(src2, FileMode.Open, FileAccess.Read, FileShare.Read);
                            currentSrc = src2;
                        }

                        if (dstHandle == null || currentDst != dst2)
                        {
                            dstHandle?.Dispose();
                            dstHandle = File.OpenHandle(
                                        dst2,
                                        FileMode.OpenOrCreate,
                                        FileAccess.ReadWrite,
                                        FileShare.ReadWrite,
                                        FileOptions.WriteThrough);
                            currentDst = dst2;
                        }

                        //await RandomAccess.ReadAsync(srcHandle, buffer, offset);
                        await RandomAccess.ReadAsync(srcHandle, buffer.AsMemory(0, size), offset);
                        await RandomAccess.WriteAsync(dstHandle, buffer.AsMemory(0, size), offset);

                        //
                        completedMaps[dst2][index2] = true;
                        stateWriters[dst2].MarkDirty();

                        Interlocked.Add(ref totalWritten, size);
                        RenderProgress(totalWritten, totalBytes, sw.Elapsed);
                    }

                    srcHandle?.Dispose();
                    dstHandle?.Dispose();
                }));
            }

            // ✅ WAIT PER FILE
            await Task.WhenAll(workers);
        }

        // Check to see if we should verify the copy by comparing hashes of the source and destination files
        if (verify)
        {
            Console.WriteLine("\nVerifying files...");
            foreach (var src in files)
            {
                string rel = Path.GetRelativePath(sourceDir, src);
                string dst = Path.Combine(destDir, rel);

                if (await HashFile(src) != await HashFile(dst))
                    Console.WriteLine($"❌ {rel}");
                else
                    Console.WriteLine($"✅ {rel}");
            }
        }

        //// ✅ Ensure all file locks are released before disposal
        //foreach (var sem in fileWriteLocks.Values)
        //{
        //    await sem.WaitAsync();
        //    sem.Release();
        //}

        // Cleanup
        foreach (var swr in stateWriters.Values)
        {
            swr.FlushNow();   // ✅ force write
            swr.Dispose();    // ✅ then shutdown
        }

        //foreach (var h in srcHandles.Values)
        //    if (h.IsValueCreated) h.Value.Dispose();

        //foreach (var h in dstHandles.Values)
        //    if (h.IsValueCreated) h.Value.Dispose();

        // ✅ FIXED: Only delete state file if fully complete
        foreach (var kvp in fileMeta)
        {
            var dst = kvp.Key;
            var (src, chunkCount, completed) = kvp.Value;

            if (completed.Count == chunkCount)
            {
                TryDeleteFile(dst + ".state.json");
            }
        }
    }

    // =========================
    // SINGLE FILE (GLOBAL STYLE)
    // =========================
    static async Task CopyFileGlobal(string source, string dest, int chunkSize, int parallel, bool resume, bool verify, string filter = "*")
    {
        await CopyDirectoryGlobal(
            Path.GetDirectoryName(source)!,
            Path.GetDirectoryName(dest)!,
            chunkSize,
            parallel,
            resume,
            verify,
            filter);
    }

    /// <summary>
    /// Progress 'Bar' indicator for console output, showing percentage, bytes copied, speed, and ETA. 
    /// This is a simple implementation that updates in place on the console.
    /// </summary>
    /// <param name="current">The number of bytes copied so far</param>
    /// <param name="total">The total number of bytes to copy</param>
    /// <param name="elapsed">The elapsed time since the copy started</param>
    static void RenderProgress(long current, long total, TimeSpan elapsed)
    {
        // Set up a simple progress bar with percentage, bytes copied, speed, and ETA
        double pct = (double)current / total;
        int width = 30;
        int fill = (int)(pct * width);

        // Set up the bar
        string bar = new string('#', fill) + new string('-', width - fill);

        // Calculate speed and ETA
        double speed = elapsed.TotalSeconds > 0 ? current / elapsed.TotalSeconds : 0;
        double eta = speed > 0 ? (total - current) / speed : 0;

        // Output
        Console.Write(
            $"\r[{bar}] {pct * 100,6:F2}% | {Fmt(current)}/{Fmt(total)} | {Fmt((long)speed)}/s | ETA {TimeSpan.FromSeconds(eta):hh\\:mm\\:ss}".PadRight(Console.WindowWidth));
    }

    /// <summary>
    /// Formats a byte count as a human-readable string using appropriate size units.
    /// </summary>
    /// <remarks>The method uses binary units, where 1 KB equals 1024 bytes. The result omits decimal places
    /// for whole numbers and includes up to two decimal places for fractional values.</remarks>
    /// <param name="b">The number of bytes to format. Must be zero or greater.</param>
    /// <returns>A string representing the byte count in the largest appropriate unit (B, KB, MB, GB, or TB), rounded to two
    /// decimal places as needed.</returns>
    static string Fmt(long b)
    {
        string[] s = ["B", "KB", "MB", "GB", "TB"];
        double v = b;
        int i = 0;
        while (v >= 1024 && i < s.Length - 1) { v /= 1024; i++; }
        return $"{v:0.##}{s[i]}";
    }

    /// <summary>
    /// Computes the SHA-256 hash of the contents of the specified file asynchronously and returns the result as a
    /// lowercase hexadecimal string.
    /// </summary>
    /// <remarks>This method reads the entire file into memory to compute the hash. If the file is large,
    /// consider the potential impact on memory usage. The returned string contains only hexadecimal digits and does not
    /// include any separators.</remarks>
    /// <param name="path">The path to the file to hash. The file must exist and be accessible for reading.</param>
    /// <returns>A lowercase hexadecimal string representing the SHA-256 hash of the file's contents.</returns>
    static async Task<string> HashFile(string path)
    {
        using var sha = SHA256.Create();
        using var stream = File.OpenRead(path);
        var hash = await sha.ComputeHashAsync(stream);
        return Convert.ToHexStringLower(hash);
    }

    /// <summary>
    /// Attempts to delete the specified file, retrying several times if the file is temporarily unavailable.
    /// </summary>
    /// <remarks>If the file cannot be deleted due to an I/O error, the method retries up to five times with a
    /// short delay between attempts. No exception is thrown if the file cannot be deleted after all retries.</remarks>
    /// <param name="path">The full path of the file to delete. Cannot be null or empty.</param>
    static void TryDeleteFile(string path)
    {
        // Attempt to delete the file, retrying if there's an IOException (e.g., file is locked)
        for (int i = 0; i < 5; i++)
        {
            try
            {
                if (File.Exists(path))
                    File.Delete(path);
                return;
            }
            catch (IOException)
            {
                Thread.Sleep(200);
            }
        }
    }

    static string NormalizeTitle(string name)
    {
        // Remove extension if present
        name = Path.GetFileNameWithoutExtension(name);

        // Remove {tmdb-xxx} or similar
        name = CurlyBracesRegex().Replace(name, "");

        // Remove [1080p], [HDR], etc.
        name = SquareBracketsRegex().Replace(name, "");

        // Remove trailing junk like " - something"
        // name = Regex.Replace(name, @"\s-\s.*$", "");

        // Normalize whitespace
        name = WhitespaceRegex().Replace(name, " ").Trim();

        return name;
    }

    static void InitializeDestinationFile(string path, long size)
    {
        try
        {
            using var fs = new FileStream(
                path,
                FileMode.OpenOrCreate,
                FileAccess.Write,
                FileShare.ReadWrite);

            // Preallocate full size
            fs.SetLength(size);
        }
        catch
        {
            // Fallback: try sparse file (best effort)
            try
            {
                using var fs = new FileStream(
                    path,
                    FileMode.OpenOrCreate,
                    FileAccess.Write,
                    FileShare.ReadWrite);

                // Mark sparse (Windows only, silently ignored elsewhere)
                fs.Seek(size - 1, SeekOrigin.Begin);
                fs.WriteByte(0);
            }
            catch
            {
                // Ignore completely — not critical
            }
        }
    }

    static async Task<bool> IsChunkWritten(SafeFileHandle srcHandle, SafeFileHandle dstHandle, long offset, int size)
    {
        int sampleSize = Math.Min(4096, size);

        byte[] srcBuffer = new byte[sampleSize];
        byte[] dstBuffer = new byte[sampleSize];

        try
        {
            await RandomAccess.ReadAsync(srcHandle, srcBuffer, offset);
            await RandomAccess.ReadAsync(dstHandle, dstBuffer, offset);
        }
        catch
        {
            return false;
        }

        return srcBuffer.AsSpan().SequenceEqual(dstBuffer);
    }

    /// <summary>
    /// Gets a compiled regular expression that matches text enclosed in curly braces.
    /// </summary>
    /// <returns>A Regex instance that matches content within curly braces using non-greedy matching.</returns>
    [GeneratedRegex(@"\{.*?\}")]
    private static partial Regex CurlyBracesRegex();

    /// <summary>
    /// Gets a compiled regular expression that matches text enclosed in square brackets, including the brackets.
    /// </summary>
    /// <returns>A <see cref="Regex"/> that matches patterns of the form [text].</returns>
    [GeneratedRegex(@"\[.*?\]")]
    private static partial Regex SquareBracketsRegex();

    /// <summary>
    /// Gets a compiled regular expression that matches one or more whitespace characters.
    /// </summary>
    /// <returns>A regular expression that matches one or more whitespace characters.</returns>
    [GeneratedRegex(@"\s+")]
    private static partial Regex WhitespaceRegex();
}