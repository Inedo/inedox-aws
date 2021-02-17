using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Inedo.Extensibility.FileSystems;
using Inedo.IO;

namespace Inedo.ProGet.Extensions.AWS.PackageStores
{
    internal sealed class S3ResumableUploadStream : UploadStream
    {
        private const long MinPartSize = 5 * 1024 * 1024;
        private readonly S3FileSystem fileSystem;
        private readonly AmazonS3Client client;
        private readonly TemporaryStream tempStream = new();
        private readonly List<PartETag> parts = new();
        private readonly string uploadId;
        private readonly string key;

        public S3ResumableUploadStream(S3FileSystem fileSystem, AmazonS3Client client, string key, string uploadId) : this(fileSystem, client, key)
        {
            this.uploadId = uploadId;
        }
        public S3ResumableUploadStream(S3FileSystem fileSystem, AmazonS3Client client, string key, byte[] state) : this(fileSystem, client, key)
        {
            using var buffer = new MemoryStream(state, false);
            using (var reader = new BinaryReader(buffer, InedoLib.UTF8Encoding, true))
            {
                this.uploadId = reader.ReadString();

                int partCount = reader.ReadInt32();
                for (int i = 1; i <= partCount; i++)
                    this.parts.Add(new PartETag(i, reader.ReadString()));
            }

            buffer.CopyTo(tempStream);
        }
        private S3ResumableUploadStream(S3FileSystem fileSystem, AmazonS3Client client, string key)
        {
            this.fileSystem = fileSystem;
            this.client = client;
            this.key = key;
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            this.tempStream.Write(buffer, offset, count);
            this.IncrementBytesWritten(count);
        }
        public override void WriteByte(byte value)
        {
            this.tempStream.WriteByte(value);
            this.IncrementBytesWritten(1);
        }
        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            await this.tempStream.WriteAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
            this.IncrementBytesWritten(count);
        }
#if !NET452
        public override void Write(ReadOnlySpan<byte> buffer)
        {
            this.tempStream.Write(buffer);
            this.IncrementBytesWritten(buffer.Length);
        }
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await this.tempStream.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
            this.IncrementBytesWritten(buffer.Length);
        }
        public override ValueTask DisposeAsync() => this.tempStream.DisposeAsync();
#endif

        public override async Task<byte[]> CommitAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                bool copy;
                this.tempStream.Position = 0;

                if (this.tempStream.Length >= MinPartSize)
                {
                    var part = await this.client.UploadPartAsync(
                        new UploadPartRequest
                        {
                            BucketName = this.fileSystem.BucketName,
                            Key = this.key,
                            UploadId = this.uploadId,
                            PartNumber = this.parts.Count + 1,
                            PartSize = this.tempStream.Length,
                            IsLastPart = false,
                            InputStream = this.tempStream
                        },
                        cancellationToken
                    ).ConfigureAwait(false);

                    this.parts.Add(new PartETag(this.parts.Count + 1, part.ETag));
                    copy = false;
                }
                else
                {
                    copy = true;
                }

                using var buffer = new MemoryStream();
                using (var writer = new BinaryWriter(buffer, InedoLib.UTF8Encoding, true))
                {
                    writer.Write(this.uploadId);
                    writer.Write(this.parts.Count);
                    foreach (var p in this.parts)
                        writer.Write(p.ETag);
                }

                if (copy)
                    this.tempStream.CopyTo(buffer);

                return buffer.ToArray();
            }
            catch
            {
                await this.CancelAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }
        }

        public async Task CompleteAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                this.tempStream.Position = 0;

                var part = await this.client.UploadPartAsync(
                    new UploadPartRequest
                    {
                        BucketName = this.fileSystem.BucketName,
                        Key = this.key,
                        UploadId = this.uploadId,
                        PartNumber = this.parts.Count + 1,
                        PartSize = this.tempStream.Length,
                        IsLastPart = true,
                        InputStream = this.tempStream
                    }
                ).ConfigureAwait(false);

                this.parts.Add(new PartETag(this.parts.Count + 1, part.ETag));

                await this.client.CompleteMultipartUploadAsync(
                    new CompleteMultipartUploadRequest
                    {
                        BucketName = this.fileSystem.BucketName,
                        Key = this.key,
                        UploadId = this.uploadId,
                        PartETags = parts
                    }
                ).ConfigureAwait(false);
            }
            catch
            {
                await this.CancelAsync(cancellationToken).ConfigureAwait(false);
                throw;
            }
        }
        public async Task CancelAsync(CancellationToken cancellationToken = default)
        {
            try
            {
                await this.client.AbortMultipartUploadAsync(
                    new AbortMultipartUploadRequest
                    {
                        BucketName = this.fileSystem.BucketName,
                        Key = this.key,
                        UploadId = this.uploadId
                    },
                    cancellationToken
                ).ConfigureAwait(false);
            }
            catch
            {
            }
        }

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                this.tempStream.Dispose();

            base.Dispose(disposing);
        }
    }
}
