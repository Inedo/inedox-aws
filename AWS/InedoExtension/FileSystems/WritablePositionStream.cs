using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Inedo.ProGet.Extensions.AWS.PackageStores
{
    /// <summary>
    /// Temporary workaround for a bug in ProGet 5.3.25 that tries to read the Length property of a stream.
    /// </summary>
    internal sealed class WritablePositionStream : Stream
    {
        private readonly Stream baseStream;
        private long position;

        public WritablePositionStream(Stream baseStream) => this.baseStream = baseStream ?? throw new ArgumentNullException(nameof(baseStream));

        public override bool CanRead => this.baseStream.CanRead;
        public override bool CanSeek => this.baseStream.CanSeek;
        public override bool CanWrite => this.baseStream.CanWrite;
        public override long Length => this.position;
        public override long Position
        {
            get => this.position;
            set
            {
                this.baseStream.Position = value;
                this.position = value;
            }
        }

        public override void Flush() => this.baseStream.Flush();
        public override int Read(byte[] buffer, int offset, int count)
        {
            int length = this.baseStream.Read(buffer, offset, count);
            this.position += length;
            return length;
        }
        public override int ReadByte()
        {
            int value = this.baseStream.ReadByte();
            if (value >= 0)
                this.position++;

            return value;
        }
        public override long Seek(long offset, SeekOrigin origin)
        {
            this.position = this.baseStream.Seek(offset, origin);
            return this.position;
        }
        public override void SetLength(long value) => this.baseStream.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count)
        {
            this.baseStream.Write(buffer, offset, count);
            this.position += count;
        }
        public override void WriteByte(byte value)
        {
            this.baseStream.WriteByte(value);
            this.position++;
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            int length = await this.baseStream.ReadAsync(buffer, offset, count).ConfigureAwait(false);
            this.position += length;
            return length;
        }
        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
        {
            await this.baseStream.WriteAsync(buffer, offset, count).ConfigureAwait(false);
            this.position += count;
        }
        public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
        {
            if (destination == null)
                throw new ArgumentNullException(nameof(destination));
            if (bufferSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(bufferSize));

            var buffer = new byte[bufferSize];
            int bytesRead = await this.baseStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false);
            while (bytesRead > 0)
            {
                this.position += bytesRead;
                await destination.WriteAsync(buffer, 0, bytesRead, cancellationToken).ConfigureAwait(false);
                bytesRead = await this.baseStream.ReadAsync(buffer, 0, buffer.Length, cancellationToken).ConfigureAwait(false);
            }
        }
        public override Task FlushAsync(CancellationToken cancellationToken) => this.baseStream.FlushAsync(cancellationToken);

#if !NET452
        public override void CopyTo(Stream destination, int bufferSize)
        {
            if (destination == null)
                throw new ArgumentNullException(nameof(destination));
            if (bufferSize <= 0)
                throw new ArgumentOutOfRangeException(nameof(bufferSize));

            var buffer = new byte[bufferSize];
            int bytesRead = this.baseStream.Read(buffer, 0, buffer.Length);
            while (bytesRead > 0)
            {
                this.position += bytesRead;
                destination.Write(buffer, 0, bytesRead);
                bytesRead = this.baseStream.Read(buffer, 0, buffer.Length);
            }
        }
        public override int Read(Span<byte> buffer)
        {
            int length = this.baseStream.Read(buffer);
            this.position += length;
            return length;
        }
        public override void Write(ReadOnlySpan<byte> buffer)
        {
            this.baseStream.Write(buffer);
            this.position += buffer.Length;
        }
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            int length = await this.baseStream.ReadAsync(buffer).ConfigureAwait(false);
            this.position += length;
            return length;
        }
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await this.baseStream.WriteAsync(buffer).ConfigureAwait(false);
            this.position += buffer.Length;
        }
#endif

        protected override void Dispose(bool disposing)
        {
            if (disposing)
                this.baseStream.Dispose();

            base.Dispose(disposing);
        }
    }
}
