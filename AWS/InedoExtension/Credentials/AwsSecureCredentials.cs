using System.ComponentModel;
using System.Security;
using Inedo.Documentation;
using Inedo.Extensibility.Credentials;
using Inedo.Serialization;
using Inedo.Web;

namespace Inedo.Extensions.AWS.Credentials
{
    [DisplayName("AWS")]
    [Description("Credentials that represent an access key ID and secret key.")]
    public sealed class AwsSecureCredentials : SecureCredentials
    {
        [Persistent]
        [DisplayName("Access key")]
        [Required]
        public string AccessKeyId { get; set; }

        [Persistent(Encrypted = true)]
        [FieldEditMode(FieldEditMode.Password)]
        [DisplayName("Secret access key")]
        [Required]
        public SecureString SecretAccessKey { get; set; }

        public override RichDescription GetDescription() => new("Access key: ", new Hilite(this.AccessKeyId));
    }
}
