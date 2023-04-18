using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Inedo.Diagnostics;
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
        private static readonly LazyRegex LegacyEscapingRegex = new(@"[&$\x00-\x1f\x7f@=;:+ ,?\\{^}%`\]""'>\[~<#|!]");
        private static readonly LazyRegex CleanPattern = new("(![0-9A-F][0-9A-F])+", RegexOptions.Compiled);
        private static readonly LazyRegex MultiSlashPattern = new("/{2,}", RegexOptions.Compiled);

        private Lazy<AmazonS3Client> client;
        private bool disposed;

        public S3FileSystem() => this.client = new Lazy<AmazonS3Client>(this.CreateClient);

        [Persistent]
        [DisplayName("Access key")]
        public string AccessKey { get; set; }
        [Persistent(Encrypted = true)]
        [DisplayName("Secret access key")]
        [FieldEditMode(FieldEditMode.Password)]
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
        [HideFromImporter]
        [Category("Storage")]
        [DisplayName("Make public")]
        public bool MakePublic { get; set; }
        [Persistent]
        [HideFromImporter]
        [Category("Storage")]
        [DisplayName("Use server-side encryption")]
        public bool Encrypted { get; set; }
        [Persistent]
        [HideFromImporter]
        [Category("Advanced")]
        [DisplayName("IAM Role")]
        [Description("This overrides the access key and secret key; only available on EC2 instances or ECS Tasks.")]
        public string InstanceRole { get; set; }

        [Persistent]
        [HideFromImporter]
        [Category("Advanced")]
        [DisplayName("Use Instance Profile")]
        [Description("When using an IAM Role, this indicates if the role is an instance profile.")]
        public bool UseInstanceProfile { get; set; } = true;

        [Persistent]
        [HideFromImporter]
        [Category("Advanced")]
        [DisplayName("Custom service URL")]
        [Description("Specifying a custom service URL will override the region endpoint.")]
        public string CustomServiceUrl { get; set; }

        internal S3CannedACL CannedACL => this.MakePublic ? S3CannedACL.PublicRead : S3CannedACL.NoACL;
        internal S3StorageClass StorageClass => S3StorageClass.Standard;
        internal ServerSideEncryptionMethod EncryptionMethod => this.Encrypted ? ServerSideEncryptionMethod.AES256 : ServerSideEncryptionMethod.None;
        internal AmazonS3Client Client
        {
            get
            {
                if (this.disposed)
                    throw new ObjectDisposedException(nameof(S3FileSystem));

                try
                {
                    return this.client.Value;
                }
                catch (ObjectDisposedException)
                {
                    // Attempt to work around an error condition of the S3 libraries that can cause
                    // the client to get prematurely disposed.
                    this.client = new Lazy<AmazonS3Client>(this.CreateClient);
                    return this.client.Value;
                }
            }
        }

        private string Prefix => string.IsNullOrEmpty(this.TargetPath) || this.TargetPath.EndsWith("/") ? this.TargetPath ?? string.Empty : (this.TargetPath + "/");

        public override async ValueTask<bool> FileExistsAsync(string fileName, CancellationToken cancellationToken = default)
        {
            var path = fileName?.Trim('/');
            if (string.IsNullOrEmpty(path))
                return false;

            return (await this.GetObjectMetadataAsync(this.BuildPath(path), cancellationToken).ConfigureAwait(false)) is not null;
        }
        public override async ValueTask<bool> DirectoryExistsAsync(string directoryName, CancellationToken cancellationToken = default)
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

            var path = this.BuildPath(fileName);

            if (hints.HasFlag(FileAccessHints.RandomAccess))
            {
                var metadata = await this.GetObjectMetadataAsync(path, cancellationToken).ConfigureAwait(false);
                if (metadata == null)
                    throw new FileNotFoundException("File not found: " + fileName, fileName);

                return new BufferedStream(new RandomAccessS3Stream(this, metadata, this.BucketName, path), 32 * 1024);
            }
            else
            {
                return (await this.GetObjectAsync(path).ConfigureAwait(false))
                    ?? throw new FileNotFoundException("File not found: " + fileName, fileName);
            }
        }
        public override async Task<Stream> CreateFileAsync(string fileName, FileAccessHints hints = FileAccessHints.Default, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));

            var client = this.Client;
            var key = this.BuildPath(fileName);

            // if there was a legacy escaped path, delete it now since it should be logically overwritten by this
            if (GetLegacyEscapedPath(key, out var escapedPath))
            {
                try
                {
                    await this.DeleteFileAsync(escapedPath, cancellationToken).ConfigureAwait(false);
                }
                catch
                {
                }
            }

            try
            {
                return new S3WriteStream(this, client, key);
            }
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }
        }

        public override async Task DeleteFileAsync(string fileName, CancellationToken cancellationToken = default)
        {
            var client = this.Client;

            try
            {
                await client.DeleteObjectAsync(
                    new DeleteObjectRequest
                    {
                        BucketName = this.BucketName,
                        Key = this.BuildPath(fileName)
                    },
                    cancellationToken
                ).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // The file does not exist, so the deletion technically succeeded.
            }
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }
        }
        public override async Task CopyFileAsync(string sourceName, string targetName, bool overwrite, CancellationToken cancellationToken = default)
        {
            var client = this.Client;

            if (!overwrite)
            {
                try
                {
                    var metadata = await client.GetObjectMetadataAsync(
                        new GetObjectMetadataRequest
                        {
                            BucketName = this.BucketName,
                            Key = this.BuildPath(targetName),
                        },
                        cancellationToken
                    ).ConfigureAwait(false);

                    throw new IOException("Destination file exists, but overwrite is not allowed: " + targetName);
                }
                catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
                {
                    // The file does not exist yet, so we can continue.
                }
                catch (AmazonS3Exception ex)
                {
                    Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                    throw;
                }
            }

            try
            {
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
                    },
                    cancellationToken
                ).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }
        }
        public override async Task CreateDirectoryAsync(string directoryName, CancellationToken cancellationToken = default)
        {
            try
            {
                var path = directoryName?.Trim('/');

                if (string.IsNullOrEmpty(path))
                    return;

                if (await this.DirectoryExistsAsync(path, cancellationToken).ConfigureAwait(false))
                    return;

                await this.CreateDirectoryInternalAsync(path).ConfigureAwait(false);

                // create other dirs just for good measure
                var parts = path.Split('/');
                for (int i = 1; i < parts.Length; i++)
                {
                    var dirPath = string.Join("/", parts.Take(i));
                    await this.CreateDirectoryAsync(dirPath, cancellationToken).ConfigureAwait(false);
                }
            }
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }
        }
        public override async Task DeleteDirectoryAsync(string directoryName, bool recursive, CancellationToken cancellationToken = default)
        {
            if (!recursive)
                return;

            var client = this.Client;

            while (true)
            {
                try
                {
                    var files = await client.ListObjectsV2Async(
                        new ListObjectsV2Request
                        {
                            BucketName = this.BucketName,
                            Prefix = this.BuildPath(directoryName) + "/"
                        },
                        cancellationToken
                    ).ConfigureAwait(false);

                    if (!files.S3Objects.Any())
                        break;

                    await client.DeleteObjectsAsync(
                        new DeleteObjectsRequest
                        {
                            BucketName = this.BucketName,
                            Objects = files.S3Objects.Select(o => new KeyVersion { Key = o.Key }).ToList()
                        },
                        cancellationToken
                    ).ConfigureAwait(false);
                }
                catch (AmazonS3Exception ex)
                {
                    Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                    throw;
                }
            }
        }
        public override async IAsyncEnumerable<FileSystemItem> ListContentsAsync(string path, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            var client = this.Client;
            var prefix = this.BuildPath(path) + "/";
            if (prefix == "/")
                prefix = string.Empty;

            var contents = new List<S3FileSystemItem>();
            var seenDirectory = new HashSet<string>();
            string continuationToken = null;
            do
            {
                ListObjectsV2Response response;
                try
                {
                    response = await client.ListObjectsV2Async(
                        new ListObjectsV2Request
                        {
                            BucketName = this.BucketName,
                            Prefix = prefix,
                            Delimiter = "/",
                            ContinuationToken = continuationToken
                        },
                        cancellationToken
                    ).ConfigureAwait(false);
                }
                catch (AmazonS3Exception ex)
                {
                    Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                    throw;
                }

                continuationToken = response.IsTruncated ? response.NextContinuationToken : null;

                if (response.CommonPrefixes != null)
                    seenDirectory.UnionWith(response.CommonPrefixes.Select(parseCommonPrefix));

                foreach (var file in response.S3Objects)
                {
                    var key = OriginalPath(file.Key[prefix.Length..]);
                    if (!string.IsNullOrEmpty(key))
                    {
                        int slash = key.IndexOf('/');
                        if (slash != -1)
                        {
                            var dir = key[..slash];
                            seenDirectory.Add(dir);
                        }
                        else
                        {
                            yield return new S3FileSystemItem(file, key);
                        }
                    }
                }
            }
            while (continuationToken != null);

            foreach (var d in seenDirectory)
                yield return new S3FileSystemItem(d);

            static string parseCommonPrefix(string p)
            {
                var path = p.Trim('/');
                int index = path.LastIndexOf('/');
                return index >= 0 ? path[(index + 1)..] : path;
            }
        }
        public override async Task<FileSystemItem> GetInfoAsync(string path, CancellationToken cancellationToken = default)
        {
            var metadata = await this.GetObjectMetadataAsync(this.BuildPath(path), cancellationToken).ConfigureAwait(false);
            if (metadata is not null)
                return new S3FileSystemItem(PathEx.GetFileName(path), metadata);

            return await this.GetDirectoryAsync(path).ConfigureAwait(false);
        }
        public override async ValueTask<long?> GetDirectoryContentSizeAsync(string path, bool recursive, CancellationToken cancellationToken = default)
        {
            var client = this.Client;
            var prefix = this.BuildPath(path) + "/";
            if (prefix == "/")
                prefix = string.Empty;

            long size = 0;
            string continuationToken = null;
            try
            {
                do
                {
                    var response = await client.ListObjectsV2Async(
                        new ListObjectsV2Request
                        {
                            BucketName = this.BucketName,
                            Prefix = prefix,
                            Delimiter = !recursive ? "/" : null,
                            ContinuationToken = continuationToken
                        },
                        cancellationToken
                    ).ConfigureAwait(false);

                    continuationToken = response.IsTruncated ? response.NextContinuationToken : null;

                    size += response.S3Objects.Sum(o => o.Size);
                }
                while (continuationToken != null);
            }
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }

            return size;
        }

        public override async Task<UploadStream> BeginResumableUploadAsync(string fileName, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));

            var client = this.Client;
            var key = this.BuildPath(fileName);

            try
            {
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
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }
        }
        public override Task<UploadStream> ContinueResumableUploadAsync(string fileName, byte[] state, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));

            var client = this.Client;
            var key = this.BuildPath(fileName);
            try
            {
                return Task.FromResult<UploadStream>(new S3ResumableUploadStream(this, client, key, state));
            }
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }
        }
        public override async Task CompleteResumableUploadAsync(string fileName, byte[] state, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));

            var client = this.Client;
            var key = this.BuildPath(fileName);

            try
            {
                using var stream = new S3ResumableUploadStream(this, client, key, state);
                await stream.CompleteAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }
        }
        public override async Task CancelResumableUploadAsync(string fileName, byte[] state, CancellationToken cancellationToken = default)
        {
            if (string.IsNullOrEmpty(fileName))
                throw new ArgumentNullException(nameof(fileName));

            var client = this.Client;
            var key = this.BuildPath(fileName);

            try
            {
                using var stream = new S3ResumableUploadStream(this, client, key, state);
                await stream.CancelAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }
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

        private async Task<GetObjectMetadataResponse> GetObjectMetadataAsync(string key, CancellationToken cancellationToken = default)
        {
            var client = this.Client;
            try
            {
                return await client.GetObjectMetadataAsync(
                    new GetObjectMetadataRequest
                    {
                        BucketName = this.BucketName,
                        Key = key
                    },
                    cancellationToken
                ).ConfigureAwait(false);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // check for an item with a legacy escaped path
                if (GetLegacyEscapedPath(key, out var escapedPath))
                {
                    try
                    {
                        return await client.GetObjectMetadataAsync(
                            new GetObjectMetadataRequest
                            {
                                BucketName = this.BucketName,
                                Key = escapedPath
                            },
                            cancellationToken
                        ).ConfigureAwait(false);
                    }
                    catch
                    {
                    }
                }

                return null;
            }
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }
        }
        private async Task<Stream> GetObjectAsync(string key)
        {
            var client = this.Client;
            try
            {
                var response = await client.GetObjectAsync(
                    new GetObjectRequest
                    {
                        BucketName = this.BucketName,
                        Key = key
                    }
                ).ConfigureAwait(false);

                return new PositionStream(response.ResponseStream, response.ContentLength);
            }
            catch (AmazonS3Exception ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // check for an item with a legacy escaped path
                if (GetLegacyEscapedPath(key, out var escapedPath))
                {
                    try
                    {
                        var response = await client.GetObjectAsync(
                            new GetObjectRequest
                            {
                                BucketName = this.BucketName,
                                Key = escapedPath
                            }
                        ).ConfigureAwait(false);

                        return new PositionStream(response.ResponseStream, response.ContentLength);
                    }
                    catch
                    {
                    }
                }

                return null;
            }
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }
        }
        private async Task<S3FileSystemItem> GetDirectoryAsync(string path)
        {
            var metadata = await this.GetObjectMetadataAsync(this.BuildPath(path) + "/").ConfigureAwait(false);
            if (metadata is not null)
                return new S3FileSystemItem(PathEx.GetFileName(path));

            return null;
        }
        private async Task CreateDirectoryInternalAsync(string path)
        {
            var client = this.Client;
            try
            {
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
            catch (AmazonS3Exception ex)
            {
                Logger.Log(MessageLevel.Debug, $"Amazon S3 Exception; Request Id: {ex.RequestId}; {ex.Message}", "Amazon S3", ex.ToString(), ex);
                throw;
            }
        }

        private AWSCredentials? CreateCredentials()
        {
            if ((!string.IsNullOrEmpty(this.AccessKey)) && (!string.IsNullOrEmpty(this.SecretAccessKey)))
                return new BasicAWSCredentials(this.AccessKey, this.SecretAccessKey);

            if ((this.UseInstanceProfile) && (!string.IsNullOrEmpty(this.InstanceRole)))
                return new InstanceProfileAWSCredentials(this.InstanceRole);

            return null;
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

        private AmazonS3Client CreateClient()
        {
            var creds = this.CreateCredentials();

            if (creds == null)
                return new(this.CreateS3Config());
            else
                return new(creds, this.CreateS3Config());
        }

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

        private static bool GetLegacyEscapedPath(string path, out string escapedPath)
        {
            if (LegacyEscapingRegex.IsMatch(path))
            {
                escapedPath = LegacyEscapingRegex.Replace(path, m => "!" + ((byte)m.Value[0]).ToString("X2"));
                return true;
            }

            escapedPath = null;
            return false;
        }

        private sealed class S3FileSystemItem : FileSystemItem
        {
            public S3FileSystemItem(string name)
            {
                this.Name = name;
                this.Size = null;
                this.IsDirectory = true;
            }
            public S3FileSystemItem(S3Object file, string key)
            {
                this.Name = PathEx.GetFileName(key);
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
