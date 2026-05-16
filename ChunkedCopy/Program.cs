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
        bool initFileFlag = false;

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
                case "--init-files":
                    initFileFlag = true;
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
            // Check for filter
            if (!String.IsNullOrWhiteSpace(filter))
            {
                await CopyDirectoryGlobal(source, dest, chunkSize, parallel, resume, verify, initFileFlag, filter);
            }
            else
            {
                await CopyDirectoryGlobal(source, dest, chunkSize, parallel, resume, verify, initFileFlag);
            }
        }
        else
        {
            // Check for filter
            if (!String.IsNullOrWhiteSpace(filter))
            {
                await CopyFileGlobal(source, dest, chunkSize, parallel, resume, verify, initFileFlag, filter);
            }
            else
            {
                await CopyFileGlobal(source, dest, chunkSize, parallel, resume, verify, initFileFlag);
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

    /// <summary>
    /// Asynchronously copies a directory recursively using chunked parallel copying with optional resume and
    /// verification support.
    /// </summary>
    /// <remarks>Files smaller than or equal to the chunk size are copied directly without chunking. State
    /// files are created to track progress and enable resumption.</remarks>
    /// <param name="sourceDir">Source directory path.</param>
    /// <param name="destDir">Destination directory path.</param>
    /// <param name="chunkSize">Size in bytes of each chunk for parallel copying.</param>
    /// <param name="parallel">Number of parallel worker tasks to use.</param>
    /// <param name="resume">Whether to resume from a previous interrupted copy using state files.</param>
    /// <param name="verify">Whether to verify copied files by comparing hash values after completion.</param>
    /// <param name="initFileFlag">Indicates whether to initialize the destination file.</param>
    /// <param name="filter">File search pattern to filter which files to copy.</param>
    /// <returns>A task that represents the asynchronous copy operation.</returns>
    static async Task CopyDirectoryGlobal(string sourceDir, string destDir, int chunkSize, int parallel, 
        bool resume, bool verify, bool initFileFlag, string filter = "*")
    {
        // Init
        Console.WriteLine($"Skipping non-\"{filter}\" file");
        string[] files = Directory.GetFiles(sourceDir, filter, SearchOption.AllDirectories);


        // Calculate total bytes for progress reporting
        long totalBytes = 0;
        foreach (var f in files)
            totalBytes += new FileInfo(f).Length;

        // Set up counters
        long totalWritten = 0;
        int index = 0;

        // Update console with total files and size
        Console.WriteLine($"Files: {files.Length} | Total: {Fmt(totalBytes)}\n");

        // Start stopwatch
        var sw = Stopwatch.StartNew();

        // Set up queues and dictionaries
        var stateWriters = new ConcurrentDictionary<string, StateWriter>();
        var completedMaps = new ConcurrentDictionary<string, ConcurrentDictionary<int, bool>>();
        // File metadata (fixes resume + avoids dst dependency)
        var fileMeta = new ConcurrentDictionary<string, (string src, int chunkCount, ConcurrentDictionary<int, bool> completed)>();
        var maxWrittenOffsets = new ConcurrentDictionary<string, long>();

        // Build global chunk queue
        foreach (var src in files)
        {
            // Init
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
            // Check to see if we want to init the files ahead of time
            if(initFileFlag) InitializeDestinationFile(dst, fileSize);

            // Compare the normalized file titles
            if (NormalizeTitle(src) == NormalizeTitle(dst))
            {
                // If the file exists and is the correct length, skip it
                if (File.Exists(dst) && (new FileInfo(dst).Length == fileSize))
                {
                    continue;
                }
            }

            // If the file doesn't exist, create it
            if (!File.Exists(dst))
            {
                using var fs = new FileStream(dst, FileMode.CreateNew, FileAccess.Write, FileShare.ReadWrite);
            }

            // Small file chunk skip
            if (fileSize <= chunkSize)
            {
                // Check the resume flag
                if (resume && File.Exists(dst))
                {
                    // Check to see if we wrote the whole file
                    if (new FileInfo(dst).Length == fileSize)
                    {
                        // Update
                        Interlocked.Add(ref totalWritten, fileSize);
                        RenderProgress(totalWritten, totalBytes, sw.Elapsed);
                        continue;
                    }
                }

                // If the file doesn't exist or isn't the right size
                if (!File.Exists(dst) || new FileInfo(dst).Length != fileSize)
                {
                    File.Copy(src, dst, true);
                }

                // Update
                Interlocked.Add(ref totalWritten, fileSize);
                RenderProgress(totalWritten, totalBytes, sw.Elapsed);

                // Keep on keeping on
                continue;
            }

            // Calculate chunk count and set up completed dictionary for this file
            int chunkCount = (int)Math.Ceiling((double)fileSize / chunkSize);
            var completed = new ConcurrentDictionary<int, bool>();
            string stateFile = dst + ".state.json";

            // Debug
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

                    // Update where we are at
                    foreach (var i in saved)
                        completed[i] = true;
                }
                catch { /* Silent Ignore */ }
            }

            // Initialize maxWrittenOffsets from state
            if (!completed.IsEmpty)
            {
                long max = (long)(completed.Keys.Max() + 1) * chunkSize;

                maxWrittenOffsets[dst] = max;
            }
            else
            {
                maxWrittenOffsets.TryAdd(dst, 0);
            }

            // Set up state
            completedMaps[dst] = completed;
            stateWriters[dst] = new StateWriter(stateFile, completed);
            fileMeta[dst] = (src, chunkCount, completed);

            // Loop through each chunk and process in parallel
            for (int i = 0; i < chunkCount; i++)
            {
                // Calculate offsets and size
                long offset = (long)i * chunkSize;
                int size = (int)Math.Min(chunkSize, fileSize - offset);

                // Check to see if we have processed this already
                bool alreadyDone = completed.ContainsKey(i);
                if (alreadyDone)
                    continue;

                // Save it
                chunkQueue.Enqueue((src, dst, offset, size, i));
            }

            // Set up workers
            var workers = new List<Task>();

            // Loop through workers
            for (int w = 0; w < parallel; w++)
            {
                // Add a worker
                workers.Add(Task.Run(async () =>
                {
                    // Set up buffer
                    byte[] buffer = new byte[chunkSize];

                    // Init
                    SafeFileHandle? srcHandle = null;
                    SafeFileHandle? dstHandle = null;
                    string? currentSrc = null;
                    string? currentDst = null;

                    // While we have chunks
                    while (chunkQueue.TryDequeue(out var job))
                    {
                        // Get the job
                        (string src2, string dst2, long offset, int size, int index2) = job;

                        // Check if we should move on from the source
                        if (srcHandle == null || currentSrc != src2)
                        {
                            srcHandle?.Dispose();
                            srcHandle = File.OpenHandle(src2, FileMode.Open, FileAccess.Read, FileShare.Read);
                            currentSrc = src2;
                        }

                        // Check to see if we can move on from the destination
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

                        // Read and Write
                        await RandomAccess.ReadAsync(srcHandle, buffer.AsMemory(0, size), offset);
                        await RandomAccess.WriteAsync(dstHandle, buffer.AsMemory(0, size), offset);

                        // Save state
                        completedMaps[dst2][index2] = true;
                        stateWriters[dst2].MarkDirty();

                        // Update UI, etc
                        Interlocked.Add(ref totalWritten, size);
                        RenderProgress(totalWritten, totalBytes, sw.Elapsed);
                    }

                    // Clean up
                    srcHandle?.Dispose();
                    dstHandle?.Dispose();
                }));
            }

            // Wait per file
            await Task.WhenAll(workers);
        }

        // Check to see if we should verify the copy by comparing hashes of the source and destination files
        if (verify)
        {
            // Debug
            Console.WriteLine("\nVerifying files...");

            // Loop through all the files
            foreach (var src in files)
            {
                // Get the paths
                string rel = Path.GetRelativePath(sourceDir, src);
                string dst = Path.Combine(destDir, rel);

                // Compare hashes
                if (await HashFile(src) != await HashFile(dst))
                    Console.WriteLine($"❌ {rel}");
                else
                    Console.WriteLine($"✅ {rel}");
            }
        }

        // Cleanup
        foreach (var swr in stateWriters.Values)
        {
            swr.FlushNow();   // ✅ force write
            swr.Dispose();    // ✅ then shutdown
        }

        // Only delete state file if fully complete
        foreach (var kvp in fileMeta)
        {
            // Init
            var dst = kvp.Key;
            var (src, chunkCount, completed) = kvp.Value;

            // Make sure that we are done
            if (completed.Count == chunkCount)
            {
                TryDeleteFile(dst + ".state.json");
            }
        }
    }

    /// <summary>
    /// Copies a file from the source path to the destination path using directory-level copy operations.
    /// </summary>
    /// <param name="source">The path of the source file to copy.</param>
    /// <param name="dest">The path of the destination file.</param>
    /// <param name="chunkSize">The size of data chunks to use during the copy operation.</param>
    /// <param name="parallel">The number of parallel operations to perform.</param>
    /// <param name="resume">Indicates whether to resume a partial copy if it exists.</param>
    /// <param name="verify">Indicates whether to verify the copied data.</param>
    /// <param name="initFileFlag">Indicates whether to initialize the destination file.</param>
    /// <param name="filter">The file filter pattern to use. Defaults to "*".</param>
    /// <returns>A task that represents the asynchronous copy operation.</returns>
    static async Task CopyFileGlobal(string source, string dest, int chunkSize, int parallel, 
        bool resume, bool verify, bool initFileFlag, string filter = "*")
    {
        // Call the directory method (I think this might not work right)
        // TODO: Verify this works correctly
        await CopyDirectoryGlobal(
            Path.GetDirectoryName(source)!,
            Path.GetDirectoryName(dest)!,
            chunkSize,
            parallel,
            resume,
            verify, 
            initFileFlag, 
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

    /// <summary>
    /// Normalizes a title string by removing file extensions, metadata tags in 
    /// curly braces and square brackets, and extra whitespace.
    /// </summary>
    /// <param name="name">The title string to normalize.</param>
    /// <returns>The normalized title string.</returns>
    protected static string NormalizeTitle(string name)
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

        // Return it
        return name;
    }

    /// <summary>
    /// Creates or opens a file at the specified path and preallocates it to the given size.
    /// </summary>
    /// <remarks>Attempts to preallocate the full size using <see cref="FileStream.SetLength"/>. If that
    /// fails, falls back to creating a sparse file on supported platforms. Errors are silently 
    /// ignored as this is a best-effort optimization.</remarks>
    /// <param name="path">The file path to create or open.</param>
    /// <param name="size">The size in bytes to preallocate for the file.</param>
    protected static void InitializeDestinationFile(string path, long size)
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

    /// <summary>
    /// Verifies whether a chunk has been written to the destination by comparing sampled data from the source and
    /// destination files.
    /// </summary>
    /// <remarks>Samples up to 4096 bytes from the chunk for comparison. Returns <see langword="false" /> if
    /// reading from either file fails.</remarks>
    /// <param name="srcHandle">The source file handle.</param>
    /// <param name="dstHandle">The destination file handle.</param>
    /// <param name="offset">The byte offset in the files where the chunk starts.</param>
    /// <param name="size">The size of the chunk to verify.</param>
    /// <returns><see langword="true" /> if the sampled data matches between source and destination; otherwise, <see
    /// langword="false" />.</returns>
    protected static async Task<bool> IsChunkWritten(SafeFileHandle srcHandle, SafeFileHandle dstHandle, long offset, int size)
    {
        // Set up sample size
        int sampleSize = Math.Min(4096, size);

        // Set up buffer
        byte[] srcBuffer = new byte[sampleSize];
        byte[] dstBuffer = new byte[sampleSize];

        try
        {
            // Read both src and dst
            await RandomAccess.ReadAsync(srcHandle, srcBuffer, offset);
            await RandomAccess.ReadAsync(dstHandle, dstBuffer, offset);
        }
        catch
        {
            // No good, so we will say this chunk isn't completed
            return false;
        }

        // Return the result
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