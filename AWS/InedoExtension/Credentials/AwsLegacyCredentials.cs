using System.ComponentModel;
using System.Security;
using Inedo.Documentation;
using Inedo.Extensibility;
using Inedo.Extensibility.Credentials;
using Inedo.Serialization;
using Inedo.Web;

namespace Inedo.Extensions.AWS.Credentials
{
    [ScriptAlias("AWS")]
    [DisplayName("(Legacy) AWS")]
    [Description("Credentials that represent an access key ID and secret key.")]
    [PersistFrom("Inedo.BuildMasterExtensions.Amazon.Credentials.AwsCredentials,Amazon")]
    public sealed class AwsCredentials : ResourceCredentials
    {
        [Persistent]
        [DisplayName("Access key")]
        public string AccessKeyId { get; set; }
        [Persistent(Encrypted = true)]
        [FieldEditMode(FieldEditMode.Password)]
        [DisplayName("Secret access key")]
        public SecureString SecretAccessKey { get; set; }

        public override RichDescription GetDescription() => new RichDescription("Access key: ", new Hilite(this.AccessKeyId));

        public override SecureCredentials ToSecureCredentials() => string.IsNullOrEmpty(this.AccessKeyId) ? null : new AwsSecureCredentials
        {
            AccessKeyId = this.AccessKeyId,
            SecretAccessKey = this.SecretAccessKey
        };
    }
}
