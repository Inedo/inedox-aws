using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Inedo.Extensibility.FileSystems;
using Inedo.IO;
using Inedo.Serialization;
using Inedo.Web;

namespace Inedo.ProGet.Extensions.AWS.PackageStores
{
    [DisplayName("Amazon S3")]
    [Description("Stores packages and assets in an Amazon S3 bucket.")]
    [PersistFrom("Inedo.ProGet.Extensions.PackageStores.S3.S3PackageStore,ProGetCoreEx")]
    [PersistFrom("Inedo.ProGet.Extensions.Amazon.PackageStores.S3PackageStore,Amazon")]
    [PersistFrom("Inedo.ProGet.Extensions.Amazon.PackageStores.S3FileSystem,Amazon")]
    [PersistFrom("Inedo.ProGet.Extensions.Amazon.PackageStores.S3FileSystem,AWS")]
    [CustomEditor(typeof(S3FileSystemEditor))]
    public sealed class S3FileSystem : FileSystem
    {
        // http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html, sections "Characters That Might Require Special Handling" and "Characters to Avoid"
        private static readonly LazyRegex UncleanPattern = new LazyRegex(@"[&$\x00-\x1f\x7f@=;:+ ,?\\{^}%`\]""'>\[~<#|!]");
        private static readonly LazyRegex CleanPattern = new LazyRegex(@"(![0-9A-F][0-9A-F])+");
        private static readonly LazyRegex MultiSlashPattern = new LazyRegex(@"/{2,}");

        private readonly LazyDisposableAsync<AmazonS3Client> client;
        private bool disposed;

        public S3FileSystem()
        {
            this.client = new LazyDisposableAsync<AmazonS3Client>(this.CreateClient, this.CreateClientAsync);
        }

        [Persistent]
        public string InstanceRole { get; set; }

        [Persistent]
        public string AccessKey { get; set; }

        [Persistent(Encrypted = true)]
        public string SecretAccessKey { get; set; }

        [Persistent]
        public string BucketName { get; set; }

        [Persistent]
        public string TargetPath { get; set; }

        [Persistent]
        public bool ReducedRedundancy { get; set; }

        [Persistent]
        public bool MakePublic { get; set; }

        [Persistent]
        public bool Encrypted { get; set; }

        [Persistent]
        public string RegionEndpoint { get; set; }

        [Persistent]
        public string CustomServiceUrl { get; set; }

        private S3CannedACL CannedACL => this.MakePublic ? S3CannedACL.PublicRead : S3CannedACL.NoACL;
        private S3StorageClass StorageClass => this.ReducedRedundancy ? S3StorageClass.ReducedRedundancy : S3StorageClass.Standard;
        private ServerSideEncryptionMethod EncryptionMethod => this.Encrypted ? ServerSideEncryptionMethod.AES256 : ServerSideEncryptionMethod.None;
        private string Prefix => string.IsNullOrEmpty(this.TargetPath) || this.TargetPath.EndsWith("/") ? this.TargetPath ?? string.Empty : (this.TargetPath + "/");

        public override async Task<bool> FileExistsAsync(string fileName)
        {
            var path = fileName?.Trim('/');
            if (string.IsNullOrEmpty(path))
                return false;

            var client = await this.client.ValueAsync.ConfigureAwait(false);

            try
            {
                await client.GetObjectMetadataAsync(new GetObjectMetadataRequest
                {
                    BucketName = this.BucketName,
                    Key = this.BuildPath(path)
                }).ConfigureAwait(false);

                return true;
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return false;
            }
        }
        public override async Task<bool> DirectoryExistsAsync(string directoryName)
        {
            var path = directoryName?.Trim('/');

            if (string.IsNullOrEmpty(path))
                return true;

            if (await this.GetDirectoryAsync(path).ConfigureAwait(false) != null)
                return true;

            return false;
        }

        public override async Task<Stream> OpenFileAsync(string fileName, FileMode mode, FileAccess access, FileShare share, bool requireRandomAccess)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));
            var client = await this.client.ValueAsync.ConfigureAwait(false);
            var key = this.BuildPath(fileName);

            if (mode == FileMode.Open && access == FileAccess.Read && !requireRandomAccess)
            {
                try
                {
                    // Fast path: just download as a stream
                    var response = await client.GetObjectAsync(new GetObjectRequest
                    {
                        BucketName = this.BucketName,
                        Key = key
                    }).ConfigureAwait(false);

                    return response.ResponseStream;
                }
                catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    try
                    {
                        // for versions of ProGet before v5.3.11
                        var response = await client.GetObjectAsync(new GetObjectRequest
                        {
                            BucketName = this.BucketName,
                            Key = this.BuildPath(fileName.ToLowerInvariant())
                        }).ConfigureAwait(false);
                        key = this.BuildPath(fileName);
                    }
                    catch (AmazonS3Exception iex) when (iex.StatusCode == HttpStatusCode.NotFound)
                    {
                        throw new FileNotFoundException("File not found: " + fileName, fileName, iex);
                    }
                }
            }

            bool? wantExisting;
            bool loadExisting;
            bool seekToEnd;
            switch (mode)
            {
                case FileMode.CreateNew:
                    wantExisting = true;
                    loadExisting = false;
                    seekToEnd = false;
                    break;
                case FileMode.Create:
                    wantExisting = null;
                    loadExisting = false;
                    seekToEnd = false;
                    break;
                case FileMode.Open:
                    wantExisting = true;
                    loadExisting = true;
                    seekToEnd = false;
                    break;
                case FileMode.OpenOrCreate:
                    wantExisting = false;
                    loadExisting = true;
                    seekToEnd = false;
                    break;
                case FileMode.Truncate:
                    wantExisting = true;
                    loadExisting = false;
                    seekToEnd = false;
                    break;
                case FileMode.Append:
                    wantExisting = null;
                    loadExisting = true;
                    seekToEnd = true;
                    break;
                default:
                    throw new NotSupportedException("Unsupported FileMode: " + mode.ToString());
            }

            Stream stream = null;

            if (loadExisting)
            {
                try
                {
                    var response = await client.GetObjectAsync(new GetObjectRequest
                    {
                        BucketName = this.BucketName,
                        Key = key
                    }).ConfigureAwait(false);

                    using (var responseStream = response.ResponseStream)
                    {
                        stream = TemporaryStream.Create(response.ContentLength);
                        try
                        {
                            await responseStream.CopyToAsync(stream).ConfigureAwait(false);
                        }
                        catch
                        {
                            try { stream.Dispose(); } catch { }
                            stream = null;
                            throw;
                        }
                    }
                }
                catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    if (wantExisting == true)
                    {
                        throw new FileNotFoundException("File not found: " + fileName, fileName, ex);
                    }
                }
            }
            else if (wantExisting.HasValue)
            {
                try
                {
                    var response = await client.GetObjectMetadataAsync(new GetObjectMetadataRequest
                    {
                        BucketName = this.BucketName,
                        Key = key
                    }).ConfigureAwait(false);

                    if (wantExisting == false)
                    {
                        throw new IOException("File already exists: " + fileName);
                    }
                }
                catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    if (wantExisting == true)
                    {
                        throw new FileNotFoundException("File not found: " + fileName, fileName, ex);
                    }
                }
            }

            if (stream == null)
            {
                stream = TemporaryStream.Create(10 * 1024 * 1024);
            }
            else
            {
                try
                {
                    stream.Seek(0, seekToEnd ? SeekOrigin.End : SeekOrigin.Begin);
                }
                catch
                {
                    try { stream.Dispose(); } catch { }
                    throw;
                }
            }

            return new S3Stream(this, stream, key, (access & FileAccess.Write) != 0);
        }
        public override async Task DeleteFileAsync(string fileName)
        {
            var client = await this.client.ValueAsync.ConfigureAwait(false);

            try
            {
                await client.DeleteObjectAsync(new DeleteObjectRequest
                {
                    BucketName = this.BucketName,
                    Key = this.BuildPath(fileName)
                }).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // The file does not exist, so the deletion technically succeeded.
            }
            try
            {
                await client.DeleteObjectAsync(new DeleteObjectRequest
                {
                    BucketName = this.BucketName,
                    Key = this.BuildPath(fileName.ToLower())
                }).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // The file does not exist, so the deletion technically succeeded.
            }
        }
        public override async Task CopyFileAsync(string sourceName, string targetName, bool overwrite)
        {
            var client = await this.client.ValueAsync.ConfigureAwait(false);

            if (!overwrite)
            {
                try
                {
                    var metadata = await client.GetObjectMetadataAsync(new GetObjectMetadataRequest
                    {
                        BucketName = this.BucketName,
                        Key = this.BuildPath(targetName),
                    }).ConfigureAwait(false);

                    throw new IOException("Destination file exists, but overwrite is not allowed: " + targetName);
                }
                catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    // The file does not exist yet, so we can continue.
                }
            }

            await client.CopyObjectAsync(new CopyObjectRequest
            {
                SourceBucket = this.BucketName,
                SourceKey = this.BuildPath(sourceName),
                DestinationBucket = this.BucketName,
                DestinationKey = this.BuildPath(targetName),
                CannedACL = this.CannedACL,
                ServerSideEncryptionMethod = this.EncryptionMethod,
                StorageClass = this.StorageClass
            }).ConfigureAwait(false);
        }
        public override async Task CreateDirectoryAsync(string directoryName)
        {
            var path = directoryName?.Trim('/');

            if (string.IsNullOrEmpty(path))
                return;

            if (await this.DirectoryExistsAsync(path).ConfigureAwait(false))
                return;

            await this.CreateDirectoryInternalAsync(path).ConfigureAwait(false);

            // create other dirs just for good measure
            var parts = path.Split('/');
            for (int i = 1; i < parts.Length; i++)
            {
                var dirPath = string.Join("/", parts.Take(i));
                await this.CreateDirectoryAsync(dirPath).ConfigureAwait(false);
            }
        }
        public override async Task DeleteDirectoryAsync(string directoryName, bool recursive)
        {
            if (!recursive)
            {
                return;
            }

            var client = await this.client.ValueAsync.ConfigureAwait(false);

            while (true)
            {
                var files = await client.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = this.BucketName,
                    Prefix = this.BuildPath(directoryName) + "/"
                }).ConfigureAwait(false);

                if (!files.S3Objects.Any())
                {
                    break;
                }

                await client.DeleteObjectsAsync(new DeleteObjectsRequest
                {
                    BucketName = this.BucketName,
                    Objects = files.S3Objects.Select(o => new KeyVersion { Key = o.Key }).ToList()
                }).ConfigureAwait(false);
            }
        }
        public override async Task<IEnumerable<FileSystemItem>> ListContentsAsync(string path)
        {
            var client = await this.client.ValueAsync.ConfigureAwait(false);
            var prefix = this.BuildPath(path) + "/";
            if (prefix == "/")
                prefix = string.Empty;

            var contents = new List<S3FileSystemItem>();
            var seenDirectory = new HashSet<string>();
            string continuationToken = null;

            do
            {
                var response = await client.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = this.BucketName,
                    Prefix = prefix,
                    Delimiter = "/",
                    ContinuationToken = continuationToken
                }).ConfigureAwait(false);

                continuationToken = response.IsTruncated ? response.NextContinuationToken : null;

                if (response.CommonPrefixes != null)
                    seenDirectory.UnionWith(response.CommonPrefixes.Select(parseCommonPrefix));

                foreach (var file in response.S3Objects)
                {
                    var key = this.OriginalPath(file.Key.Substring(prefix.Length));
                    var slash = key.IndexOf('/');
                    if (slash != -1)
                    {
                        var dir = key.Substring(0, slash);
                        seenDirectory.Add(dir);
                    }
                    else
                    {
                        contents.Add(new S3FileSystemItem(key, file.Size));
                    }
                }
            }
            while (continuationToken != null);

            return seenDirectory.Select(d => new S3FileSystemItem(d)).Concat(contents);

            static string parseCommonPrefix(string p)
            {
                var path = p.Trim('/');
                int index = path.LastIndexOf('/');
                return index >= 0 ? path.Substring(index + 1) : path;
            }
        }
        public override async Task<FileSystemItem> GetInfoAsync(string path)
        {
            var client = await this.client.ValueAsync.ConfigureAwait(false);

            try
            {
                var metadata = await client.GetObjectMetadataAsync(new GetObjectMetadataRequest
                {
                    BucketName = this.BucketName,
                    Key = this.BuildPath(path)
                }).ConfigureAwait(false);

                return new S3FileSystemItem(PathEx.GetFileName(path), metadata.ContentLength);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return await this.GetDirectoryAsync(path).ConfigureAwait(false);
            }
        }

        private async Task<S3FileSystemItem> GetDirectoryAsync(string path)
        {
            var client = await this.client.ValueAsync.ConfigureAwait(false);

            try
            {
                var metadata = await client.GetObjectMetadataAsync(
                    new GetObjectMetadataRequest
                    {
                        BucketName = this.BucketName,
                        Key = this.BuildPath(path) + "/"
                    }
                ).ConfigureAwait(false);

                return new S3FileSystemItem(PathEx.GetFileName(path));
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        private async Task CreateDirectoryInternalAsync(string path)
        {
            var client = await this.client.ValueAsync.ConfigureAwait(false);
            await client.PutObjectAsync(
                new PutObjectRequest
                {
                    BucketName = this.BucketName,
                    Key = this.BuildPath(path) + "/",
                    InputStream = Stream.Null,
                    CannedACL = this.CannedACL,
                    StorageClass = this.StorageClass,
                    ServerSideEncryptionMethod = this.EncryptionMethod
                }
            ).ConfigureAwait(false);
        }

        protected override void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    this.client.Dispose();
                }

                this.disposed = true;
            }

            base.Dispose(disposing);
        }

        private AWSCredentials CreateCredentials()
        {
            if (string.IsNullOrEmpty(this.InstanceRole))
            {
                return new BasicAWSCredentials(this.AccessKey, this.SecretAccessKey);
            }

            return new InstanceProfileAWSCredentials(this.InstanceRole);
        }
        private AmazonS3Config CreateS3Config()
        {
            // https://docs.aws.amazon.com/sdk-for-net/v3/developer-guide/net-dg-region-selection.html
            // example service URL is: https://ec2.us-west-new.amazonaws.com

            if (!string.IsNullOrEmpty(this.CustomServiceUrl))
                return new AmazonS3Config { ServiceURL = this.CustomServiceUrl };
            else
                return new AmazonS3Config { RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(this.RegionEndpoint) };
        }
        private AmazonS3Client CreateClient() => new AmazonS3Client(this.CreateCredentials(), this.CreateS3Config());
        private Task<AmazonS3Client> CreateClientAsync() => Task.Run(() => this.CreateClient());
        private string BuildPath(string path)
        {
            // Replace the "dirty" ASCII characters with an exclamation mark followed by two hex digits.
            path = UncleanPattern.Replace(path, m => "!" + ((byte)m.Value[0]).ToString("X2"));

            // Collapse slashes.
            path = MultiSlashPattern.Replace(path.Trim('/'), "");

            return (this.Prefix + path)?.Trim('/');
        }
        private string OriginalPath(string path)
        {
            return CleanPattern.Replace(path, m => InedoLib.UTF8Encoding.GetString(
                m.Value.Split(new[] { '!' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(b => Convert.ToByte(b, 16)).ToArray()));
        }

        private sealed class S3FileSystemItem : FileSystemItem
        {
            public S3FileSystemItem(string name)
            {
                this.Name = name;
                this.Size = null;
                this.IsDirectory = true;
            }

            public S3FileSystemItem(string name, long size)
            {
                this.Name = name;
                this.Size = size;
                this.IsDirectory = false;
            }

            public override string Name { get; }
            public override long? Size { get; }
            public override bool IsDirectory { get; }
        }

        private sealed class S3Stream : Stream
        {
            private readonly S3FileSystem outer;
            private readonly Stream inner;
            private readonly string key;

            public S3Stream(S3FileSystem outer, Stream inner, string key, bool canWrite)
            {
                this.outer = outer;
                this.inner = inner;
                this.key = key;
                this.CanWrite = canWrite;
            }

            public override bool CanRead => true;
            public override bool CanSeek => true;
            public override bool CanWrite { get; }
            public override long Length => this.inner.Length;
            public override long Position
            {
                get => this.inner.Position;
                set => this.inner.Position = value;
            }

            public override void Flush()
            {
                // no-op
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                return this.inner.Read(buffer, offset, count);
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                return this.inner.Seek(offset, origin);
            }

            public override void SetLength(long value)
            {
                this.inner.SetLength(value);
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                this.inner.Write(buffer, offset, count);
            }

            public override Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
            {
                return this.inner.CopyToAsync(destination, bufferSize, cancellationToken);
            }

            public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return this.inner.ReadAsync(buffer, offset, count, cancellationToken);
            }

            public override int ReadByte()
            {
                return this.inner.ReadByte();
            }

            public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            {
                return this.inner.WriteAsync(buffer, offset, count, cancellationToken);
            }

            public override void WriteByte(byte value)
            {
                this.inner.WriteByte(value);
            }

            protected override void Dispose(bool disposing)
            {
                if (disposing)
                {
                    using (this.inner)
                    {
                        if (this.CanWrite)
                        {
                            this.FinishUploadAsync().GetAwaiter().GetResult();
                        }
                    }
                }
                base.Dispose(disposing);
            }

            private async Task FinishUploadAsync()
            {
                var client = await this.outer.client.ValueAsync.ConfigureAwait(false);

                if (this.inner.Length < 4L * 1024 * 1024 * 1024)
                {
                    await client.PutObjectAsync(new PutObjectRequest
                    {
                        BucketName = this.outer.BucketName,
                        Key = this.key,
                        StorageClass = this.outer.StorageClass,
                        CannedACL = this.outer.CannedACL,
                        ServerSideEncryptionMethod = this.outer.EncryptionMethod,
                        AutoCloseStream = false,
                        InputStream = this.inner
                    }).ConfigureAwait(false);

                    return;
                }

                var multipart = await client.InitiateMultipartUploadAsync(new InitiateMultipartUploadRequest
                {
                    BucketName = this.outer.BucketName,
                    Key = this.key,
                    StorageClass = this.outer.StorageClass,
                    CannedACL = this.outer.CannedACL,
                    ServerSideEncryptionMethod = this.outer.EncryptionMethod
                }).ConfigureAwait(false);

                try
                {
                    var parts = new List<PartETag>();
                    long position = 0;
                    var remaining = this.inner.Length;
                    for (var partNumber = 1; remaining > 0; partNumber++)
                    {
                        var partSize = Math.Min(remaining, 1024 * 1024 * 1024);
                        this.inner.Position = position;
                        position += partSize;
                        remaining -= partSize;

                        var part = await client.UploadPartAsync(new UploadPartRequest
                        {
                            BucketName = this.outer.BucketName,
                            Key = this.key,
                            UploadId = multipart.UploadId,
                            PartNumber = partNumber,
                            PartSize = partSize,
                            IsLastPart = remaining <= 0,
                            InputStream = new UndisposableStream(this.inner)
                        }).ConfigureAwait(false);

                        parts.Add(new PartETag(partNumber, part.ETag));
                    }

                    await client.CompleteMultipartUploadAsync(new CompleteMultipartUploadRequest
                    {
                        BucketName = this.outer.BucketName,
                        Key = this.key,
                        UploadId = multipart.UploadId,
                        PartETags = parts
                    }).ConfigureAwait(false);
                }
                catch
                {
                    try
                    {
                        await client.AbortMultipartUploadAsync(new AbortMultipartUploadRequest
                        {
                            BucketName = this.outer.BucketName,
                            Key = this.key,
                            UploadId = multipart.UploadId
                        }).ConfigureAwait(false);
                    }
                    catch { }

                    throw;
                }
            }
        }
    }
}
