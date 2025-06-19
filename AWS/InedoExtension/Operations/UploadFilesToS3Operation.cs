using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Inedo.Agents;
using Inedo.Diagnostics;
using Inedo.Documentation;
using Inedo.Extensibility;
using Inedo.Extensibility.Credentials;
using Inedo.Extensibility.Operations;
using Inedo.Extensions.AWS.Credentials;
using Inedo.IO;
using Inedo.Web;

namespace Inedo.Extensions.AWS.Operations
{
    [ScriptAlias("Upload-Files")]
    [DisplayName("Upload Files to S3")]
    [Description("Transfers files to an Amazon S3 bucket.")]
    [ScriptNamespace("AWS")]
    [Tag("amazon"), Tag("cloud")]
    public sealed class UploadFilesToS3Operation : ExecuteOperation
    {
        private long totalUploadBytes;
        private long uploadedBytes;

        [ScriptAlias("From")]
        [DisplayName("Source directory")]
        [PlaceholderText("$WorkingDirectory")]
        public string SourceDirectory { get; set; }
        [ScriptAlias("Include")]
        [DisplayName("Include")]
        [DefaultValue("*")]
        [MaskingDescription]
        public IEnumerable<string> Includes { get; set; }
        [ScriptAlias("Exclude")]
        [DisplayName("Exclude")]
        [MaskingDescription]
        public IEnumerable<string> Excludes { get; set; }

        [Required]
        [ScriptAlias("Bucket")]
        [DisplayName("Bucket")]
        public string BucketName { get; set; }
        [ScriptAlias("To")]
        [DisplayName("Target folder")]
        [Description("The directory in the specified S3 bucket that will receive the uploaded files.")]
        public string KeyPrefix { get; set; }
        [ScriptAlias("ReducedRedundancy")]
        [DisplayName("Use reduced redundancy")]
        [Category("Storage")]
        public bool ReducedRedundancy { get; set; }
        [ScriptAlias("Public")]
        [DisplayName("Make public")]
        [Category("Storage")]
        public bool MakePublic { get; set; }
        [ScriptAlias("Encrypted")]
        [Category("Storage")]
        public bool Encrypted { get; set; }

        [ScriptAlias("Credentials")]
        [DisplayName("Credentials")]
        [SuggestableValue(typeof(SecureCredentialsSuggestionProvider<AwsSecureCredentials>))]
        public string CredentialName { get; set; }
        [ScriptAlias("AccessKey")]
        [DisplayName("Access key")]
        [PlaceholderText("Use credentials")]
        public string AccessKey { get; set; }
        [ScriptAlias("SecretAccessKey")]
        [DisplayName("Secret access key")]
        [PlaceholderText("Use credentials")]
        public string SecretAccessKey { get; set; }

        [Category("Network")]
        [ScriptAlias("RegionEndpoint")]
        [DisplayName("Region endpoint")]
        [SuggestableValue(typeof(RegionEndpointSuggestionProvider))]
        public string RegionEndpoint { get; set; }
        [Category("Network")]
        [ScriptAlias("PartSize")]
        [DisplayName("Part size")]
        [DefaultValue(5L * 1024 * 1024)]
        [Description("The size (in bytes) of individual parts for an S3 multipart upload.")]
        public long PartSize { get; set; } = 5L * 1024 * 1024;

        [Category("Advanced")]
        [ScriptAlias("UsePathStyle")]
        [DisplayName("Enable Path Style for S3")]
        [Description("Activate path-style URLs for accessing Amazon S3 buckets. Useful for compatibility with certain applications and services.")]
        public bool UsePathStyle { get; set; }

        [Category("Advanced")]
        [ScriptAlias("CustomServiceUrl")]
        [DisplayName("Custom service URL")]
        [Description("Specifying a custom service URL will override the region endpoint.")]
        public string CustomServiceUrl { get; set; }

        private S3CannedACL CannedACL => this.MakePublic ? S3CannedACL.PublicRead : S3CannedACL.NoACL;
        private ServerSideEncryptionMethod EncryptionMethod => this.Encrypted ? ServerSideEncryptionMethod.AES256 : ServerSideEncryptionMethod.None;
        private S3StorageClass StorageClass => this.ReducedRedundancy ? S3StorageClass.ReducedRedundancy : S3StorageClass.Standard;

        public override async Task ExecuteAsync(IOperationExecutionContext context)
        {
            var credentials = this.GetCredentials(context as ICredentialResolutionContext);
            var fileOps = await context.Agent.GetServiceAsync<IFileOperationsExecuter>();
            var sourceDirectory = context.ResolvePath(this.SourceDirectory);
            if (!await fileOps.DirectoryExistsAsync(sourceDirectory))
            {
                this.LogWarning($"Source directory {sourceDirectory} does not exist; nothing to upload.");
                return;
            }

            var files = (await fileOps.GetFileSystemInfosAsync(sourceDirectory, new MaskingContext(this.Includes, this.Excludes)))
                .OfType<SlimFileInfo>()
                .ToList();

            if (files.Count == 0)
            {
                this.LogWarning($"No files match the specified masks in {sourceDirectory}; nothing to upload.");
                return;
            }

            var prefix = string.Empty;
            if (!string.IsNullOrEmpty(this.KeyPrefix))
                prefix = this.KeyPrefix.Trim('/') + "/";

            Interlocked.Exchange(ref this.totalUploadBytes, files.Sum(f => f.Size));

            using var s3 = this.CreateClient(credentials);
            foreach (var file in files)
            {
                var keyName = prefix + file.FullName[sourceDirectory.Length..].Replace(Path.DirectorySeparatorChar, '/').Trim('/');
                this.LogInformation($"Transferring {file.FullName} to {keyName} ({AH.FormatSize(file.Size)})...");
                using var fileStream = await fileOps.OpenFileAsync(file.FullName, FileMode.Open, FileAccess.Read);
                if (file.Size < this.PartSize * 2)
                    await this.UploadSmallFileAsync(s3, fileStream, keyName, context);
                else
                    await this.MultipartUploadAsync(s3, fileStream, keyName, context);
            }
        }

        public override OperationProgress GetProgress()
        {
            long total = Interlocked.Read(ref this.totalUploadBytes);
            long uploaded = Interlocked.Read(ref this.uploadedBytes);
            if (total == 0)
                return null;

            long remaining = Math.Max(total - uploaded, 0);
            if (remaining > 0)
                return new OperationProgress((int)(100.0 * uploaded / total), AH.FormatSize(remaining) + " remaining");
            else
                return new OperationProgress(100);
        }

        protected override ExtendedRichDescription GetDescription(IOperationConfiguration config)
        {
            return new ExtendedRichDescription(
                new RichDescription(
                    "Upload ",
                    new MaskHilite(config[nameof(this.Includes)], config[nameof(this.Excludes)]),
                    " to S3"
                ),
                new RichDescription(
                    "from ",
                    new DirectoryHilite(config[nameof(this.SourceDirectory)]),
                    " to ",
                    new Hilite((config[nameof(this.BucketName)] + "/" + config[nameof(this.KeyPrefix)]).TrimEnd('/'))
                )
            );
        }

        private AwsSecureCredentials GetCredentials(ICredentialResolutionContext context)
        {
            AwsSecureCredentials credentials;
            if (string.IsNullOrEmpty(this.CredentialName))
            {
                credentials = this.AccessKey == null ? null : new AwsSecureCredentials();
            }
            else
            {
                credentials = SecureCredentials.TryCreate(this.CredentialName, context) as AwsSecureCredentials;
            }

            if (credentials != null)
            {
                credentials.AccessKeyId = AH.CoalesceString(this.AccessKey, credentials.AccessKeyId);
                credentials.SecretAccessKey = !string.IsNullOrWhiteSpace(this.SecretAccessKey) ? AH.CreateSecureString(this.SecretAccessKey) : credentials.SecretAccessKey;
            }

            return credentials;
        }

        private Task UploadSmallFileAsync(AmazonS3Client s3, Stream stream, string key, IOperationExecutionContext context)
        {
            return s3.PutObjectAsync(
                new PutObjectRequest
                {
                    BucketName = this.BucketName,
                    Key = key,
                    AutoCloseStream = false,
                    AutoResetStreamPosition = false,
                    InputStream = stream,
                    CannedACL = this.CannedACL,
                    StorageClass = this.StorageClass,
                    ServerSideEncryptionMethod = this.EncryptionMethod,
                    StreamTransferProgress = (s, e) => Interlocked.Add(ref this.uploadedBytes, e.IncrementTransferred)
                },
                context.CancellationToken
            );
        }
        private async Task MultipartUploadAsync(AmazonS3Client s3, Stream stream, string key, IOperationExecutionContext context)
        {
            var uploadResponse = await s3.InitiateMultipartUploadAsync(
                new InitiateMultipartUploadRequest
                {
                    BucketName = this.BucketName,
                    Key = key,
                    CannedACL = this.CannedACL,
                    StorageClass = this.StorageClass,
                    ServerSideEncryptionMethod = this.EncryptionMethod
                },
                context.CancellationToken
            );

            try
            {
                var parts = this.GetParts(stream.Length);

                var completedParts = new List<PartETag>(parts.Count);
                for (int i = 0; i < parts.Count; i++)
                {
                    var partResponse = await s3.UploadPartAsync(
                        new UploadPartRequest
                        {
                            BucketName = this.BucketName,
                            Key = key,
                            InputStream = stream,
                            UploadId = uploadResponse.UploadId,
                            PartSize = parts[i].Length,
                            FilePosition = parts[i].StartOffset,
                            PartNumber = i + 1,
                            StreamTransferProgress = (s, e) => Interlocked.Add(ref this.uploadedBytes, e.IncrementTransferred)
                        },
                        context.CancellationToken
                    );

                    completedParts.Add(new PartETag(i + 1, partResponse.ETag));
                }

                await s3.CompleteMultipartUploadAsync(
                    new CompleteMultipartUploadRequest
                    {
                        BucketName = this.BucketName,
                        Key = key,
                        UploadId = uploadResponse.UploadId,
                        PartETags = completedParts
                    },
                    context.CancellationToken
                );
            }
            catch
            {
                await s3.AbortMultipartUploadAsync(
                    new AbortMultipartUploadRequest
                    {
                        BucketName = this.BucketName,
                        Key = key,
                        UploadId = uploadResponse.UploadId
                    }
                );

                throw;
            }
        }
        private List<PartInfo> GetParts(long totalSize)
        {
            if (totalSize < this.PartSize * 2)
                return null;

            int wholeParts = (int)(totalSize / this.PartSize);
            var parts = new List<PartInfo>(wholeParts);

            for (int i = 0; i < wholeParts - 1; i++)
                parts.Add(new PartInfo(i * this.PartSize, this.PartSize));

            long remainder = totalSize % this.PartSize;
            parts.Add(new PartInfo((wholeParts - 1) * this.PartSize, this.PartSize + remainder));

            return parts;
        }

        private AmazonS3Client CreateClient(AwsSecureCredentials credentials) => new (credentials.AccessKeyId, AH.Unprotect(credentials.SecretAccessKey), this.CreateS3Config());

        private AmazonS3Config CreateS3Config()
        {
            // https://docs.aws.amazon.com/sdk-for-net/v3/developer-guide/net-dg-region-selection.html
            // example service URL is: https://ec2.us-west-new.amazonaws.com
            var config = new AmazonS3Config();
            if (!string.IsNullOrEmpty(this.CustomServiceUrl))
                config.ServiceURL = this.CustomServiceUrl;
            else if (!string.IsNullOrEmpty(this.RegionEndpoint))
                config.RegionEndpoint = Amazon.RegionEndpoint.GetBySystemName(this.RegionEndpoint);

            // 
            if (this.UsePathStyle)
                config.ForcePathStyle = true;

            return config;
        }

        private readonly struct PartInfo
        {
            public PartInfo(long startOffset, long length)
            {
                this.StartOffset = startOffset;
                this.Length = length;
            }

            public long StartOffset { get; }
            public long Length { get; }
        }
    }
}
