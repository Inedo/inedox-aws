using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Inedo.IO;

namespace Inedo.ProGet.Extensions.AWS.PackageStores
{
    internal sealed class S3WriteStream : Stream
    {
        private const long PartSize = 5 * 1024 * 1024;
        private readonly S3FileSystem fileSystem;
        private readonly AmazonS3Client client;
        private readonly string key;
        private readonly List<PartETag> parts = new();
        private readonly object syncLock = new();
        private TemporaryStream writeStream = new();
        private string uploadId;
        private Task uploadTask;
        private int partNumber = 1;
        private bool disposed;
        private int writeByteCalls;

        public S3WriteStream(S3FileSystem fileSystem, AmazonS3Client client, string key)
        {
            this.fileSystem = fileSystem;
            this.client = client;
            this.key = key;
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Write(byte[] buffer, int offset, int count)
        {
            this.writeStream.Write(buffer, 0, count);
            this.CheckAndBeginBackgroundUpload();
        }
        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            await this.writeStream.WriteAsync(buffer.AsMemory(0, count), cancellationToken).ConfigureAwait(false);
            this.CheckAndBeginBackgroundUpload();
        }
        public override void WriteByte(byte value)
        {
            this.writeStream.WriteByte(value);

            // don't do the check for every single byte in case this gets called a lot
            this.writeByteCalls++;
            if ((this.writeByteCalls % 1000) == 0)
                this.CheckAndBeginBackgroundUpload();
        }
        public override void Flush()
        {
        }
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(ReadOnlySpan<byte> buffer)
        {
            this.writeStream.Write(buffer);
            this.CheckAndBeginBackgroundUpload();
        }
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await this.writeStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            this.CheckAndBeginBackgroundUpload();
        }
        public override async ValueTask DisposeAsync()
        {
            if (!this.disposed)
            {
                await this.CompleteUploadAsync().ConfigureAwait(false);
                await this.writeStream.DisposeAsync().ConfigureAwait(false);

                this.disposed = true;
            }
        }

        private async Task CompleteUploadAsync()
        {
            Task waitTask;
            lock (this.syncLock)
            {
                waitTask = this.uploadTask;
            }

            if (waitTask != null)
                await waitTask.ConfigureAwait(false);

            this.writeStream.Position = 0;

            if (this.parts.Count == 0)
            {
                await client.PutObjectAsync(
                    new PutObjectRequest
                    {
                        BucketName = this.fileSystem.BucketName,
                        Key = this.key,
                        StorageClass = this.fileSystem.StorageClass,
                        CannedACL = this.fileSystem.CannedACL,
                        ServerSideEncryptionMethod = this.fileSystem.EncryptionMethod,
                        AutoCloseStream = false,
                        InputStream = this.writeStream
                    }
                ).ConfigureAwait(false);
            }
            else
            {
                try
                {
                    var part = await this.client.UploadPartAsync(
                        new UploadPartRequest
                        {
                            BucketName = this.fileSystem.BucketName,
                            Key = this.key,
                            UploadId = this.uploadId,
                            PartNumber = this.partNumber,
                            PartSize = this.writeStream.Length,
                            IsLastPart = true,
                            InputStream = this.writeStream
                        }
                    ).ConfigureAwait(false);

                    this.parts.Add(new PartETag(this.partNumber, part.ETag));

                    await client.CompleteMultipartUploadAsync(
                        new CompleteMultipartUploadRequest
                        {
                            BucketName = this.fileSystem.BucketName,
                            Key = this.key,
                            UploadId = this.uploadId,
                            PartETags = parts
                        }
                    ).ConfigureAwait(false);
                }
                catch when (this.uploadId is not null)
                {
                    try
                    {
                        await client.AbortMultipartUploadAsync(
                            new AbortMultipartUploadRequest
                            {
                                BucketName = this.fileSystem.BucketName,
                                Key = this.key,
                                UploadId = this.uploadId
                            }
                        ).ConfigureAwait(false);
                    }
                    catch
                    {
                    }

                    throw;
                }
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    this.CompleteUploadAsync().GetAwaiter().GetResult();
                    this.writeStream.Dispose();
                }

                this.disposed = true;
            }

            base.Dispose(disposing);
        }

        private void CheckAndBeginBackgroundUpload()
        {
            if (this.writeStream.Position >= PartSize)
            {
                lock (this.syncLock)
                {
                    var stream = this.writeStream;
                    this.writeStream = new TemporaryStream();

                    stream.Position = 0;
                    int partNumber = this.partNumber++;

                    if (this.uploadTask == null)
                        this.uploadTask = Task.Run(() => this.UploadPartAsync(stream, partNumber));
                    else
                        this.uploadTask = this.uploadTask.ContinueWith(_ => this.UploadPartAsync(stream, partNumber)).Unwrap();
                }
            }
        }
        private async Task UploadPartAsync(Stream source, int partNumber)
        {
            if (partNumber == 1)
            {
                var response = await this.client.InitiateMultipartUploadAsync(
                    new InitiateMultipartUploadRequest
                    {
                        BucketName = this.fileSystem.BucketName,
                        Key = this.key,
                        StorageClass = this.fileSystem.StorageClass,
                        CannedACL = this.fileSystem.CannedACL,
                        ServerSideEncryptionMethod = this.fileSystem.EncryptionMethod
                    }
                ).ConfigureAwait(false);

                this.uploadId = response.UploadId;
            }

            try
            {
                var part = await this.client.UploadPartAsync(
                    new UploadPartRequest
                    {
                        BucketName = this.fileSystem.BucketName,
                        Key = this.key,
                        UploadId = this.uploadId,
                        PartNumber = partNumber,
                        PartSize = source.Length,
                        IsLastPart = false,
                        InputStream = source
                    }
                ).ConfigureAwait(false);

                lock (this.syncLock)
                {
                    this.parts.Add(new PartETag(partNumber, part.ETag));
                }
            }
            finally
            {
                source.Dispose();
            }
        }
    }
}
