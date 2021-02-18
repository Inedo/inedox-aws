using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Inedo.Documentation;
using Inedo.Extensibility.FileSystems;
using Inedo.Extensions.AWS;
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
    public sealed class S3FileSystem : FileSystem
    {
        private static readonly LazyRegex CleanPattern = new LazyRegex(@"(![0-9A-F][0-9A-F])+", RegexOptions.Compiled);
        private static readonly LazyRegex MultiSlashPattern = new LazyRegex(@"/{2,}", RegexOptions.Compiled);

        private readonly Lazy<AmazonS3Client> client;
        private bool disposed;

        public S3FileSystem()
        {
            this.client = new Lazy<AmazonS3Client>(this.CreateClient);
        }

        [Persistent]
        [DisplayName("Access key")]
        public string AccessKey { get; set; }
        [Persistent(Encrypted = true)]
        [DisplayName("Secret access key")]
        public string SecretAccessKey { get; set; }
        [Required]
        [Persistent]
        [DisplayName("Region endpoint")]
        [SuggestableValue(typeof(RegionEndpointSuggestionProvider))]
        public string RegionEndpoint { get; set; }
        [Persistent]
        [DisplayName("Bucket")]
        public string BucketName { get; set; }
        [Persistent]
        [DisplayName("Prefix")]
        [PlaceholderText("none (use bucket root)")]
        public string TargetPath { get; set; }
        [Persistent]
        [Category("Storage")]
        [DisplayName("Make public")]
        public bool MakePublic { get; set; }
        [Persistent]
        [Category("Storage")]
        [DisplayName("Use server-side encryption")]
        public bool Encrypted { get; set; }
        [Persistent]
        [Category("Advanced")]
        [DisplayName("Instance role")]
        [Description("This overrides the access key and secret key; only available on EC2 instances.")]
        public string InstanceRole { get; set; }
        [Persistent]
        [Category("Advanced")]
        [DisplayName("Custom service URL")]
        [Description("Specifying a custom service URL will override the region endpoint.")]
        public string CustomServiceUrl { get; set; }

        internal S3CannedACL CannedACL => this.MakePublic ? S3CannedACL.PublicRead : S3CannedACL.NoACL;
        internal S3StorageClass StorageClass => S3StorageClass.Standard;
        internal ServerSideEncryptionMethod EncryptionMethod => this.Encrypted ? ServerSideEncryptionMethod.AES256 : ServerSideEncryptionMethod.None;

        private string Prefix => string.IsNullOrEmpty(this.TargetPath) || this.TargetPath.EndsWith("/") ? this.TargetPath ?? string.Empty : (this.TargetPath + "/");

        public override async Task<bool> FileExistsAsync(string fileName)
        {
            var path = fileName?.Trim('/');
            if (string.IsNullOrEmpty(path))
                return false;

            var client = this.client.Value;
            try
            {
                await client.GetObjectMetadataAsync(
                    new GetObjectMetadataRequest
                    {
                        BucketName = this.BucketName,
                        Key = this.BuildPath(path)
                    }
                ).ConfigureAwait(false);

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

        public override async Task<Stream> OpenReadAsync(string fileName, FileAccessHints hints = FileAccessHints.Default, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));

            var client = this.client.Value;
            var key = this.BuildPath(fileName);

            try
            {
                var response = await client.GetObjectAsync(
                    new GetObjectRequest
                    {
                        BucketName = this.BucketName,
                        Key = key
                    },
                    cancellationToken
                ).ConfigureAwait(false);

                return new PositionStream(response.ResponseStream, response.ContentLength);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                throw new FileNotFoundException("File not found: " + fileName, fileName, ex);
            }
        }
        public override Task<Stream> CreateFileAsync(string fileName, FileAccessHints hints = FileAccessHints.Default, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));

            var client = this.client.Value;
            var key = this.BuildPath(fileName);
            return Task.FromResult<Stream>(new S3WriteStream(this, client, key));
        }

        public override async Task DeleteFileAsync(string fileName)
        {
            var client = this.client.Value;

            try
            {
                await client.DeleteObjectAsync(
                    new DeleteObjectRequest
                    {
                        BucketName = this.BucketName,
                        Key = this.BuildPath(fileName)
                    }
                ).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // The file does not exist, so the deletion technically succeeded.
            }
        }
        public override async Task CopyFileAsync(string sourceName, string targetName, bool overwrite)
        {
            var client = this.client.Value;

            if (!overwrite)
            {
                try
                {
                    var metadata = await client.GetObjectMetadataAsync(
                        new GetObjectMetadataRequest
                        {
                            BucketName = this.BucketName,
                            Key = this.BuildPath(targetName),
                        }
                    ).ConfigureAwait(false);

                    throw new IOException("Destination file exists, but overwrite is not allowed: " + targetName);
                }
                catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    // The file does not exist yet, so we can continue.
                }
            }

            await client.CopyObjectAsync(
                new CopyObjectRequest
                {
                    SourceBucket = this.BucketName,
                    SourceKey = this.BuildPath(sourceName),
                    DestinationBucket = this.BucketName,
                    DestinationKey = this.BuildPath(targetName),
                    CannedACL = this.CannedACL,
                    ServerSideEncryptionMethod = this.EncryptionMethod,
                    StorageClass = this.StorageClass
                }
            ).ConfigureAwait(false);
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
                return;

            var client = this.client.Value;

            while (true)
            {
                var files = await client.ListObjectsV2Async(
                    new ListObjectsV2Request
                    {
                        BucketName = this.BucketName,
                        Prefix = this.BuildPath(directoryName) + "/"
                    }
                ).ConfigureAwait(false);

                if (!files.S3Objects.Any())
                    break;

                await client.DeleteObjectsAsync(
                    new DeleteObjectsRequest
                    {
                        BucketName = this.BucketName,
                        Objects = files.S3Objects.Select(o => new KeyVersion { Key = o.Key }).ToList()
                    }
                ).ConfigureAwait(false);
            }
        }
        public override async Task<IEnumerable<FileSystemItem>> ListContentsAsync(string path)
        {
            var client = this.client.Value;
            var prefix = this.BuildPath(path) + "/";
            if (prefix == "/")
                prefix = string.Empty;

            var contents = new List<S3FileSystemItem>();
            var seenDirectory = new HashSet<string>();
            string continuationToken = null;

            do
            {
                var response = await client.ListObjectsV2Async(
                    new ListObjectsV2Request
                    {
                        BucketName = this.BucketName,
                        Prefix = prefix,
                        Delimiter = "/",
                        ContinuationToken = continuationToken
                    }
                ).ConfigureAwait(false);

                continuationToken = response.IsTruncated ? response.NextContinuationToken : null;

                if (response.CommonPrefixes != null)
                    seenDirectory.UnionWith(response.CommonPrefixes.Select(parseCommonPrefix));

                foreach (var file in response.S3Objects)
                {
                    var key = OriginalPath(file.Key.Substring(prefix.Length));
                    var slash = key.IndexOf('/');
                    if (slash != -1)
                    {
                        var dir = key.Substring(0, slash);
                        seenDirectory.Add(dir);
                    }
                    else
                    {
                        contents.Add(new S3FileSystemItem(file));
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
            var client = this.client.Value;

            try
            {
                var metadata = await client.GetObjectMetadataAsync(
                    new GetObjectMetadataRequest
                    {
                        BucketName = this.BucketName,
                        Key = this.BuildPath(path)
                    }
                ).ConfigureAwait(false);

                return new S3FileSystemItem(PathEx.GetFileName(path), metadata);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return await this.GetDirectoryAsync(path).ConfigureAwait(false);
            }
        }

        public override async Task<UploadStream> BeginResumableUploadAsync(string fileName, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));

            var client = this.client.Value;
            var key = this.BuildPath(fileName);

            var response = await client.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest
                {
                    BucketName = this.BucketName,
                    Key = key,
                    StorageClass = this.StorageClass,
                    CannedACL = this.CannedACL,
                    ServerSideEncryptionMethod = this.EncryptionMethod
                },
                cancellationToken
            ).ConfigureAwait(false);

            return new S3ResumableUploadStream(this, client, key, response.UploadId);
        }
        public override Task<UploadStream> ContinueResumableUploadAsync(string fileName, byte[] state, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));

            var client = this.client.Value;
            var key = this.BuildPath(fileName);
            return Task.FromResult<UploadStream>(new S3ResumableUploadStream(this, client, key, state));
        }
        public override async Task CompleteResumableUploadAsync(string fileName, byte[] state, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));

            var client = this.client.Value;
            var key = this.BuildPath(fileName);

            using var stream = new S3ResumableUploadStream(this, client, key, state);
            await stream.CompleteAsync(cancellationToken).ConfigureAwait(false);
        }
        public override async Task CancelResumableUploadAsync(string fileName, byte[] state, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));

            var client = this.client.Value;
            var key = this.BuildPath(fileName);

            using var stream = new S3ResumableUploadStream(this, client, key, state);
            await stream.CancelAsync(cancellationToken).ConfigureAwait(false);
        }

        public override RichDescription GetDescription()
        {
            if (string.IsNullOrEmpty(this.BucketName))
                return base.GetDescription();

            return new RichDescription(
                "Amazon S3: ",
                new Hilite(this.BucketName + "://" + this.TargetPath?.TrimStart('/'))
            );
        }

        protected override void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    if (this.client.IsValueCreated)
                        this.client.Value.Dispose();
                }

                this.disposed = true;
            }

            base.Dispose(disposing);
        }

        private async Task<S3FileSystemItem> GetDirectoryAsync(string path)
        {
            var client = this.client.Value;

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
            var client = this.client.Value;
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
        private AWSCredentials CreateCredentials()
        {
            if (string.IsNullOrEmpty(this.InstanceRole))
                return new BasicAWSCredentials(this.AccessKey, this.SecretAccessKey);

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
        private string BuildPath(string path)
        {
            path = MultiSlashPattern.Replace(path.Trim('/'), string.Empty);
            return (this.Prefix + path)?.Trim('/');
        }
        private static string OriginalPath(string path)
        {
            // This should be removed at some point.
            // The AWS extension used to perform escaping of certain characters but this is no longer recommended.
            return CleanPattern.Replace(
                path,
                m => InedoLib.UTF8Encoding.GetString(m.Value.Split(new[] { '!' }, StringSplitOptions.RemoveEmptyEntries).Select(b => Convert.ToByte(b, 16)).ToArray())
            );
        }

        private sealed class S3FileSystemItem : FileSystemItem
        {
            public S3FileSystemItem(string name)
            {
                this.Name = name;
                this.Size = null;
                this.IsDirectory = true;
            }
            public S3FileSystemItem(S3Object file)
            {
                this.Name = PathEx.GetFileName(file.Key);
                this.Size = file.Size;
                this.IsDirectory = false;
                this.LastModifyTime = new DateTimeOffset(file.LastModified.ToUniversalTime(), TimeSpan.Zero);
            }
            public S3FileSystemItem(string name, GetObjectMetadataResponse file)
            {
                this.Name = name;
                this.Size = file.ContentLength;
                this.IsDirectory = false;
                this.LastModifyTime = new DateTimeOffset(file.LastModified, TimeSpan.Zero);
            }

            public override string Name { get; }
            public override long? Size { get; }
            public override bool IsDirectory { get; }
            public override DateTimeOffset? LastModifyTime { get; }
        }
    }
}
