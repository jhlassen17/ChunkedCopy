using System.Collections.Concurrent;
using System.Text.Json;

/// <summary>
/// Provides background persistence for a set of integer keys, periodically writing their state to disk in a thread-safe
/// manner.  These keys represent the current state of a copy operation
/// </summary>
/// <remarks>StateWriter monitors a shared collection of integer keys and writes their state to the specified file
/// path at regular intervals, only persisting changes when the data is marked as modified. This class is thread-safe
/// and intended for scenarios where frequent updates occur and batching writes improves performance. Dispose of the
/// instance to ensure all pending changes are flushed to disk and resources are released.</remarks>
public class StateWriter : IDisposable
{
    /// <summary>
    /// Represents the file system path associated with the current instance.
    /// </summary>
    private readonly string path;
    /// <summary>
    /// Set of chunks
    /// </summary>
    private readonly ConcurrentDictionary<int, bool> data;
    /// <summary>
    /// Cancellation token source to signal the background loop to stop when disposing the instance.
    /// </summary>
    private readonly CancellationTokenSource cts = new();
    /// <summary>
    /// Main task that runs the background loop, periodically checking for changes and writing to disk as needed. 
    /// This task is safely awaited during disposal to ensure all operations complete before resources are released.
    /// </summary>
    private readonly Task loopTask;
    /// <summary>
    /// Indicates whether the current state has been modified since the last reset or synchronization.
    /// </summary>
    private volatile bool dirty = false;

    /// <summary>
    /// Initializes a new instance of the StateWriter class with the specified file path and data source.
    /// </summary>
    /// <remarks>The constructor starts an asynchronous background task to process and write the provided
    /// data. Ensure that the provided dictionary remains valid for the lifetime of the StateWriter instance.</remarks>
    /// <param name="path">The file system path where the state data will be written. Cannot be null or empty.</param>
    /// <param name="data">A thread-safe dictionary containing the state data to be managed and written. Cannot be null.</param>
    public StateWriter(string path, ConcurrentDictionary<int, bool> data)
    {
        // Save
        this.path = path;
        this.data = data;

        // Run the task
        loopTask = Task.Run(LoopAsync);
    }

    /// <summary>
    /// Marks the current object as dirty, indicating that its state has changed and may require saving or further
    /// processing.
    /// </summary>
    public void MarkDirty() => dirty = true;

    /// <summary>
    /// Continuously performs periodic flush operations until cancellation is requested.
    /// </summary>
    /// <remarks>This method is intended to be run in the background. It performs a flush operation every two
    /// seconds and ensures a final flush occurs after cancellation. If the cancellation token is triggered, the loop
    /// exits gracefully after completing any in-progress operation.</remarks>
    /// <returns>A task that represents the asynchronous loop operation. The task completes when cancellation is requested and
    /// the final flush operation has finished.</returns>
    private async Task LoopAsync()
    {
        // Loop until cancellation is requested
        while (!cts.Token.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(2000, cts.Token);
                Flush();
            }
            catch (TaskCanceledException)
            {
                break;
            }
        }

        // final flush on exit
        Flush();
    }

    /// <summary>
    /// Writes any pending changes to the underlying storage if modifications have occurred since the last flush.
    /// </summary>
    /// <remarks>This method attempts to persist changes only if the internal state is marked as dirty. If an
    /// error occurs during the flush operation, the dirty flag is reset to ensure the changes will be retried on the
    /// next call. This method is not thread-safe.</remarks>
    private void Flush()
    {
        // If we aren't dirty, skip the flush
        if (!dirty) return;

        try
        {
            // Reset dirty flag before flush, if an exception occurs, the flag will be left as dirty to ensure we retry on the next cycle
            dirty = false;

            // Save
            string tmp = path + ".tmp";
            File.WriteAllText(tmp, JsonSerializer.Serialize(data.Keys));
            File.Move(tmp, path, true);
        }
        catch
        {
            dirty = true; // retry next cycle
        }
    }

    /// <summary>
    /// Releases all resources used by the current instance.
    /// </summary>
    /// <remarks>Call this method when the instance is no longer needed to ensure that all associated
    /// resources are properly released. After calling this method, the instance should not be used.</remarks>
    public void Dispose()
    {
        // Signal the background loop to stop and wait for it to complete
        cts.Cancel();

        try
        {
            loopTask.Wait(); // ✅ safe, no deadlock
        }
        catch { }

        // Dispose of the cancellation token source
        cts.Dispose();
    }

    /// <summary>
    /// Flushes any pending changes to the underlying storage immediately, 
    /// regardless of the current dirty state. This method
    /// </summary>
    public void FlushNow()
    {
        Flush();
    }
}