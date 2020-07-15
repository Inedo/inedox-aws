using System.Linq;
using Amazon;
using Amazon.Runtime;
using Inedo.Web;
using Inedo.Web.Controls;
using Inedo.Web.Controls.SimpleHtml;
using Inedo.Web.Editors;

namespace Inedo.ProGet.Extensions.AWS.PackageStores
{
    internal sealed class S3FileSystemEditor : FileSystemEditor
    {
        private AhTextInput txtInstanceRole = new AhTextInput();
        private AhTextInput txtAccessKey = new AhTextInput { Required = true };
        private AhPasswordInput txtSecretKey = new AhPasswordInput { Required = true };
        private AhTextInput txtBucket = new AhTextInput { Required = true };
        private AhTextInput txtPrefix = new AhTextInput { Placeholder = "none (use bucket root)" };
        private AhCheckboxInput chkReducedRedundancy = new AhCheckboxInput { LabelText = "Use reduced redundancy" };
        private AhCheckboxInput chkPublic = new AhCheckboxInput { LabelText = "Make public" };
        private AhCheckboxInput chkEncrypted = new AhCheckboxInput { LabelText = "Use server-side encryption" };
        private AhTextInput txtEndpoint = new AhTextInput
        {
            AutoCompleteValues = RegionEndpoint.EnumerableAllRegions.Select(r => r.SystemName)
        };
        private AhTextInput txtServiceUrl = new AhTextInput { Placeholder = "Automatic from region endpoint" };

        protected override ISimpleControl CreateEditorControl()
        {
            string[] roles;
            try
            {
                roles = InstanceProfileAWSCredentials.GetAvailableRoles().ToArray();
                this.txtInstanceRole.AutoCompleteValues = roles;
                this.txtAccessKey.Required = false;
                this.txtSecretKey.Required = false;
                this.txtInstanceRole.ServerValidate = ValidateHasCredentials;
                this.txtAccessKey.ServerValidate = ValidateHasCredentials;
                this.txtSecretKey.ServerValidate = ValidateHasCredentials;
                this.txtInstanceRole.ServerValidateIfNullOrEmpty = true;
                this.txtAccessKey.ServerValidateIfNullOrEmpty = true;
                this.txtSecretKey.ServerValidateIfNullOrEmpty = true;
                this.txtEndpoint.ServerValidate = ValidateEndpoint;
                this.txtServiceUrl.ServerValidate = ValidateEndpoint;
            }
            catch (AmazonServiceException)
            {
                roles = null;
            }

            return new SimplePageControl(
                new SlimFormField("Instance role:", this.txtInstanceRole,
                    new P("This overrides the access key and secret key. Only available on EC2 instances.")) { Visible = roles != null },
                new SlimFormField("Access key:", this.txtAccessKey),
                new SlimFormField("Secret key:", this.txtSecretKey),
                new SlimFormField("Bucket:", this.txtBucket),
                new SlimFormField("Prefix:", this.txtPrefix),
                new SlimFormField("Region endpoint:", this.txtEndpoint),
                new SlimFormField("Custom service URL:", this.txtServiceUrl)
                {
                    HelpText = new LiteralHtml("Specifying a custom service URL will override the region endpoint. Valid endpoints are documented here: "
                    + "<a href=\"https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region\" target=\"_blank\">https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region</a>")
                },
                new SlimFormField(
                    "Storage:",
                    new Div(this.chkReducedRedundancy),
                    new Div(this.chkPublic),
                    new Div(this.chkEncrypted)
                )
            );
        }
        public override void BindToInstance(object instance)
        {
            var s3 = (S3FileSystem)instance;
            this.txtInstanceRole.Value = s3.InstanceRole;
            this.txtAccessKey.Value = s3.AccessKey;
            this.txtSecretKey.PasswordValue = s3.SecretAccessKey;
            this.txtBucket.Value = s3.BucketName;
            this.txtPrefix.Value = s3.TargetPath;
            this.chkReducedRedundancy.Checked = s3.ReducedRedundancy;
            this.chkPublic.Checked = s3.MakePublic;
            this.chkEncrypted.Checked = s3.Encrypted;
            this.txtEndpoint.Value = s3.RegionEndpoint;
            this.txtServiceUrl.Value = s3.CustomServiceUrl;
        }
        public override void WriteToInstance(object instance)
        {
            var s3 = (S3FileSystem)instance;
            s3.InstanceRole = AH.NullIf(this.txtInstanceRole.Value, string.Empty);
            s3.AccessKey = this.txtAccessKey.Value;
            s3.SecretAccessKey = this.txtSecretKey.PasswordValue;
            s3.BucketName = this.txtBucket.Value;
            s3.TargetPath = this.txtPrefix.Value;
            s3.ReducedRedundancy = this.chkReducedRedundancy.Checked;
            s3.MakePublic = this.chkPublic.Checked;
            s3.Encrypted = this.chkEncrypted.Checked;
            s3.RegionEndpoint = this.txtEndpoint.Value;
            s3.CustomServiceUrl = this.txtServiceUrl.Value;
        }

        private ValidationResults ValidateHasCredentials(string value)
        {
            if (!string.IsNullOrEmpty(this.txtInstanceRole.Value))
                return true;

            if (!string.IsNullOrEmpty(this.txtAccessKey.Value) && !string.IsNullOrEmpty(this.txtSecretKey.PasswordValue))
                return true;

            return new ValidationResults(false, "Either instance role or access key and secret key must be set.");
        }

        private ValidationResults ValidateEndpoint(string value)
        {
            if (!string.IsNullOrEmpty(this.txtServiceUrl.Value))
                return true;

            if (!string.IsNullOrEmpty(this.txtEndpoint.Value))
                return true;

            return new ValidationResults(false, "Either region endpoint or service URL must be set.");
        }
    }
}
