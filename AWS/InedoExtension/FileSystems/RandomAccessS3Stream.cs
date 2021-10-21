using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3.Model;

namespace Inedo.ProGet.Extensions.AWS.PackageStores
{
    internal sealed class RandomAccessS3Stream : Stream
    {
        private readonly S3FileSystem fileSystem;
        private readonly GetObjectMetadataResponse objectMetadata;
        private readonly string bucketName;
        private readonly string key;
        private long position;

        public RandomAccessS3Stream(S3FileSystem fileSystem, GetObjectMetadataResponse objectMetadata, string bucketName, string key)
        {
            this.fileSystem = fileSystem;
            this.objectMetadata = objectMetadata;
            this.bucketName = bucketName;
            this.key = key;
        }

        public override bool CanRead => true;
        public override bool CanSeek => true;
        public override bool CanWrite => false;
        public override long Length => this.objectMetadata.ContentLength;

        public override long Position
        {
            get => this.position;
            set => this.position = value;
        }
        public override void Flush()
        {
        }

        public override int Read(byte[] buffer, int offset, int count) => this.ReadAsync(buffer, offset, count, default).GetAwaiter().GetResult();
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            if (buffer == null)
                throw new ArgumentNullException(nameof(buffer));
            if (offset < 0)
                throw new ArgumentNullException(nameof(offset));
            if (count < 0)
                throw new ArgumentOutOfRangeException(nameof(count));

            long endPosition = Math.Min(this.objectMetadata.ContentLength, this.position + count);
            if (this.position == endPosition)
                return 0;

            using var response = await this.fileSystem.Client.GetObjectAsync(
                new GetObjectRequest
                {
                    BucketName = this.bucketName,
                    Key = this.key,
                    EtagToMatch = this.objectMetadata.ETag,
                    ByteRange = new(this.position, endPosition)
                },
                cancellationToken
            ).ConfigureAwait(false);

            using var responseStream = response.ResponseStream;

            int bytesRead = 0;
            int bytesRemaining = (int)(endPosition - this.position);

            int n = await responseStream.ReadAsync(buffer, offset, bytesRemaining, cancellationToken).ConfigureAwait(false);
            bytesRead += n;
            bytesRemaining -= n;
            while (n > 0 && bytesRemaining > 0)
            {
                n = await responseStream.ReadAsync(buffer, offset + bytesRead, bytesRemaining, cancellationToken).ConfigureAwait(false);
                bytesRead += n;
                bytesRemaining -= n;
            }

            this.position += bytesRead;
            return bytesRead;
        }
        public override long Seek(long offset, SeekOrigin origin)
        {
            return this.position = origin switch
            {
                SeekOrigin.Begin => offset,
                SeekOrigin.Current => this.position + offset,
                SeekOrigin.End => this.objectMetadata.ContentLength + offset,
                _ => throw new ArgumentOutOfRangeException(nameof(origin))
            };
        }

        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
