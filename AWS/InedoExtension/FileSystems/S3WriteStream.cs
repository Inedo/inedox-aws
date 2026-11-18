using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Inedo.IO;

#nullable enable

namespace Inedo.ProGet.Extensions.AWS.PackageStores;

internal sealed class S3WriteStream : Stream
{
    private const int PartSize = 5 * 1024 * 1024;
    private readonly S3FileSystem fileSystem;
    private readonly AmazonS3Client client;
    private readonly string key;
    private readonly Channel<PendingPart> channel = Channel.CreateBounded<PendingPart>(new BoundedChannelOptions(10) { SingleReader = true, SingleWriter = true, FullMode = BoundedChannelFullMode.Wait });
    private readonly Task uploadTask;
    private readonly CancellationTokenSource cts = new();
    private TemporaryStream currentPartStream = new();
    private volatile Exception? fault;
    private int nextPartNumber = 1;
    private bool disposed;

    public S3WriteStream(S3FileSystem fileSystem, AmazonS3Client client, string key)
    {
        this.fileSystem = fileSystem;
        this.client = client;
        this.key = key;
        this.uploadTask = this.RunUploadLoopAsync(this.cts.Token);
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => !this.disposed && this.fault is null;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public override void Flush()
    {
    }
    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    public override void WriteByte(byte value)
    {
        this.ThrowIfDisposedOrFaulted();

        this.currentPartStream.WriteByte(value);
        if (this.currentPartStream.Length >= PartSize)
            this.FlushCurrentPart(false);
    }

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        this.ThrowIfDisposedOrFaulted();

        var remaining = buffer;
        while (!remaining.IsEmpty)
        {
            int space = PartSize - (int)this.currentPartStream.Length;
            int toWrite = Math.Min(space, remaining.Length);

            this.currentPartStream.Write(remaining[..toWrite]);
            remaining = remaining[toWrite..];

            if (this.currentPartStream.Length >= PartSize)
                this.FlushCurrentPart(false);
        }
    }
    public override void Write(byte[] buffer, int offset, int count) => this.Write(buffer.AsSpan(offset, count));

    public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        this.ThrowIfDisposedOrFaulted();

        var remaining = buffer;
        while (!remaining.IsEmpty)
        {
            int space = PartSize - (int)this.currentPartStream.Length;
            int toWrite = Math.Min(space, remaining.Length);

            await this.currentPartStream.WriteAsync(remaining[..toWrite], cancellationToken).ConfigureAwait(false);
            remaining = remaining[toWrite..];

            if (this.currentPartStream.Length >= PartSize)
                await this.FlushCurrentPartAsync(false, cancellationToken);
        }
    }
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) => this.WriteAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override async ValueTask DisposeAsync()
    {
        if (!this.disposed)
        {
            this.disposed = true;

            try
            {
                if (this.fault is not null)
                {
                    this.channel.Writer.TryComplete(this.fault);
                    try
                    {
                        await this.uploadTask.ConfigureAwait(false);
                    }
                    catch
                    {
                    }

                    throw this.fault;
                }

                await this.FlushCurrentPartAsync(true, cts.Token).ConfigureAwait(false);
                await this.uploadTask.ConfigureAwait(false);
            }
            finally
            {
                this.cts.Cancel();
                this.cts.Dispose();
                this.currentPartStream?.Dispose();
            }
        }
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
            this.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    private void ThrowIfDisposedOrFaulted()
    {
        ObjectDisposedException.ThrowIf(this.disposed, this);
        if (this.fault is not null)
            throw this.fault;
    }
    private void FlushCurrentPart(bool isLast) => this.FlushCurrentPartAsync(isLast, this.cts.Token).GetAwaiter().GetResult();
    private async Task FlushCurrentPartAsync(bool isLast, CancellationToken cancellationToken)
    {
        var streamToSend = this.currentPartStream;
        this.currentPartStream = isLast ? null! : new TemporaryStream();

        streamToSend.Position = 0;
        await this.channel.Writer.WriteAsync(new PendingPart(this.nextPartNumber, streamToSend, isLast), cancellationToken).ConfigureAwait(false);
        if (isLast)
            this.channel.Writer.Complete();

        this.nextPartNumber++;
    }

    private async Task RunUploadLoopAsync(CancellationToken cancellationToken)
    {
        string? uploadId = null;
        var completedParts = new List<PartETag>();

        try
        {
            await foreach (var (number, stream, isLast) in this.channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            {
                try
                {
                    if (uploadId is null)
                    {
                        // check if this is the only part
                        if (isLast)
                        {
                            await this.client.PutObjectAsync(
                                new PutObjectRequest
                                {
                                    BucketName = this.fileSystem.BucketName,
                                    Key = this.key,
                                    StorageClass = this.fileSystem.StorageClass,
                                    CannedACL = this.fileSystem.CannedACL,
                                    ServerSideEncryptionMethod = this.fileSystem.EncryptionMethod,
                                    AutoCloseStream = false,
                                    InputStream = stream
                                },
                                cancellationToken
                            ).ConfigureAwait(false);

                            // don't complete the multipart upload because we didn't start one
                            return;
                        }
                        else
                        {
                            var response = await this.client.InitiateMultipartUploadAsync(
                                new InitiateMultipartUploadRequest
                                {
                                    BucketName = this.fileSystem.BucketName,
                                    Key = this.key,
                                    StorageClass = this.fileSystem.StorageClass,
                                    CannedACL = this.fileSystem.CannedACL,
                                    ServerSideEncryptionMethod = this.fileSystem.EncryptionMethod
                                },
                                cancellationToken
                            ).ConfigureAwait(false);

                            uploadId = response.UploadId;
                        }
                    }

                    var partResponse = await this.client.UploadPartAsync(
                        new UploadPartRequest
                        {
                            BucketName = this.fileSystem.BucketName,
                            Key = this.key,
                            UploadId = uploadId,
                            PartNumber = number,
                            PartSize = stream.Length,
                            IsLastPart = isLast,
                            InputStream = stream
                        },
                        cancellationToken
                    ).ConfigureAwait(false);

                    completedParts.Add(new PartETag(number, partResponse.ETag));
                }
                finally
                {
                    stream.Dispose();
                }
            }

            if (uploadId is not null && completedParts.Count > 0)
            {
                await client.CompleteMultipartUploadAsync(
                    new CompleteMultipartUploadRequest
                    {
                        BucketName = this.fileSystem.BucketName,
                        Key = this.key,
                        UploadId = uploadId,
                        PartETags = completedParts
                    },
                    cancellationToken
                ).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            this.fault ??= ex;
            try
            {
                this.channel.Writer.TryComplete(ex);

                if (uploadId is not null)
                {
                    await this.client.AbortMultipartUploadAsync(
                        new AbortMultipartUploadRequest
                        {
                            BucketName = this.fileSystem.BucketName,
                            Key = this.key,
                            UploadId = uploadId
                        }
                    ).ConfigureAwait(false);
                }
            }
            catch
            {
            }

            throw;
        }
        finally
        {
            while (this.channel.Reader.TryRead(out var remainingPart))
                remainingPart.Stream.Dispose();
        }
    }

    private sealed record class PendingPart(int Number, TemporaryStream Stream, bool IsLast);
}
