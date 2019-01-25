using Amazon;
using Amazon.Runtime;
using Inedo.Web.Controls;
using Inedo.Web.Controls.SimpleHtml;
using Inedo.Web.Editors;
using System.Linq;
using System.Web.UI.WebControls;

namespace Inedo.ProGet.Extensions.AWS.PackageStores
{
    internal sealed class S3FileSystemEditor : FileSystemEditor
    {
        private ValidatingTextBox txtInstanceRole = new ValidatingTextBox();
        private ValidatingTextBox txtAccessKey = new ValidatingTextBox { Required = true };
        private PasswordTextBox txtSecretKey = new PasswordTextBox { Required = true };
        private ValidatingTextBox txtBucket = new ValidatingTextBox { Required = true };
        private ValidatingTextBox txtPrefix = new ValidatingTextBox { DefaultText = "none (use bucket root)" };
        private SimpleCheckBox chkReducedRedundancy = new SimpleCheckBox { Text = "Use reduced redundancy" };
        private SimpleCheckBox chkPublic = new SimpleCheckBox { Text = "Make public" };
        private SimpleCheckBox chkEncrypted = new SimpleCheckBox { Text = "Use server-side encryption" };
        private ValidatingTextBox txtEndpoint = new ValidatingTextBox
        {
            AutoCompleteValues = RegionEndpoint.EnumerableAllRegions.Select(r => r.SystemName)
        };
        private ValidatingTextBox txtServiceUrl = new ValidatingTextBox { DefaultText = "Automatic from region endpoint" };

        protected override ISimpleControl CreateEditorControl()
        {
            string[] roles;
            try
            {
                roles = InstanceProfileAWSCredentials.GetAvailableRoles().ToArray();
                this.txtInstanceRole.AutoCompleteValues = roles;
                this.txtAccessKey.Required = false;
                this.txtSecretKey.Required = false;
                this.txtInstanceRole.ServerValidate += ValidateHasCredentials;
                this.txtAccessKey.ServerValidate += ValidateHasCredentials;
                this.txtSecretKey.ServerValidate += ValidateHasCredentials;
                this.txtInstanceRole.ServerValidateIfNullOrEmpty = true;
                this.txtAccessKey.ServerValidateIfNullOrEmpty = true;
                this.txtSecretKey.ServerValidateIfNullOrEmpty = true;
                this.txtEndpoint.ServerValidate += ValidateEndpoint;
                this.txtServiceUrl.ServerValidate += ValidateEndpoint;
            }
            catch (AmazonServiceException)
            {
                roles = null;
            }

            return new SimpleVirtualCompositeControl(
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
            this.txtInstanceRole.Text = s3.InstanceRole;
            this.txtAccessKey.Text = s3.AccessKey;
            this.txtSecretKey.Text = s3.SecretAccessKey;
            this.txtBucket.Text = s3.BucketName;
            this.txtPrefix.Text = s3.TargetPath;
            this.chkReducedRedundancy.Checked = s3.ReducedRedundancy;
            this.chkPublic.Checked = s3.MakePublic;
            this.chkEncrypted.Checked = s3.Encrypted;
            this.txtEndpoint.Text = s3.RegionEndpoint;
            this.txtServiceUrl.Text = s3.CustomServiceUrl;
        }
        public override void WriteToInstance(object instance)
        {
            var s3 = (S3FileSystem)instance;
            s3.InstanceRole = AH.NullIf(this.txtInstanceRole.Text, string.Empty);
            s3.AccessKey = this.txtAccessKey.Text;
            s3.SecretAccessKey = this.txtSecretKey.Text;
            s3.BucketName = this.txtBucket.Text;
            s3.TargetPath = this.txtPrefix.Text;
            s3.ReducedRedundancy = this.chkReducedRedundancy.Checked;
            s3.MakePublic = this.chkPublic.Checked;
            s3.Encrypted = this.chkEncrypted.Checked;
            s3.RegionEndpoint = this.txtEndpoint.Text;
            s3.CustomServiceUrl = this.txtServiceUrl.Text;
        }

        private void ValidateHasCredentials(object source, ServerValidateEventArgs args)
        {
            if (!string.IsNullOrEmpty(this.txtInstanceRole.Text))
            {
                args.IsValid = true;
                this.txtInstanceRole.ValidatorText = null;
                return;
            }

            if (!string.IsNullOrEmpty(this.txtAccessKey.Text) && !string.IsNullOrEmpty(this.txtSecretKey.Text))
            {
                args.IsValid = true;
                this.txtInstanceRole.ValidatorText = null;
                return;
            }

            args.IsValid = false;
            this.txtInstanceRole.ValidatorText = "Either instance role or access key and secret key must be set.";
        }

        private void ValidateEndpoint(object source, ServerValidateEventArgs args)
        {
            if (!string.IsNullOrEmpty(this.txtServiceUrl.Text))
            {
                args.IsValid = true;
                this.txtServiceUrl.ValidatorText = null;
                return;
            }

            if (!string.IsNullOrEmpty(this.txtEndpoint.Text))
            {
                args.IsValid = true;
                this.txtEndpoint.ValidatorText = null;
                return;
            }

            args.IsValid = false;
            this.txtServiceUrl.ValidatorText = "Either region endpoint or service URL must be set.";
        }
    }
}
