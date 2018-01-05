using System.Linq;
using Inedo.Web.Controls;
using Inedo.Web.Controls.SimpleHtml;
using Inedo.Web.Editors;

namespace Inedo.ProGet.Extensions.Amazon.PackageStores
{
    internal sealed class S3FileSystemEditor : FileSystemEditor
    {
        private ValidatingTextBox txtAccessKey = new ValidatingTextBox { Required = true };
        private PasswordTextBox txtSecretKey = new PasswordTextBox { Required = true };
        private ValidatingTextBox txtBucket = new ValidatingTextBox { Required = true };
        private ValidatingTextBox txtPrefix = new ValidatingTextBox { DefaultText = "none (use bucket root)" };
        private SimpleCheckBox chkReducedRedundancy = new SimpleCheckBox { Text = "Use reduced redundancy" };
        private SimpleCheckBox chkPublic = new SimpleCheckBox { Text = "Make public" };
        private SimpleCheckBox chkEncrypted = new SimpleCheckBox { Text = "Use server-side encryption" };
        private ValidatingTextBox txtEndpoint = new ValidatingTextBox
        {
            Required = true,
            AutoCompleteValues = global::Amazon.RegionEndpoint.EnumerableAllRegions.Select(r => r.SystemName)
        };

        protected override ISimpleControl CreateEditorControl()
        {
            return new SimpleVirtualCompositeControl(
                new SlimFormField("Access key:", this.txtAccessKey),
                new SlimFormField("Secret key:", this.txtSecretKey),
                new SlimFormField("Bucket:", this.txtBucket),
                new SlimFormField("Prefix:", this.txtPrefix),
                new SlimFormField("Region endpoint:", this.txtEndpoint),
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
            this.txtAccessKey.Text = s3.AccessKey;
            this.txtSecretKey.Text = s3.SecretAccessKey;
            this.txtBucket.Text = s3.BucketName;
            this.txtPrefix.Text = s3.TargetPath;
            this.chkReducedRedundancy.Checked = s3.ReducedRedundancy;
            this.chkPublic.Checked = s3.MakePublic;
            this.chkEncrypted.Checked = s3.Encrypted;
            this.txtEndpoint.Text = s3.RegionEndpoint;
        }
        public override void WriteToInstance(object instance)
        {
            var s3 = (S3FileSystem)instance;
            s3.AccessKey = this.txtAccessKey.Text;
            s3.SecretAccessKey = this.txtSecretKey.Text;
            s3.BucketName = this.txtBucket.Text;
            s3.TargetPath = this.txtPrefix.Text;
            s3.ReducedRedundancy = this.chkReducedRedundancy.Checked;
            s3.MakePublic = this.chkPublic.Checked;
            s3.Encrypted = this.chkEncrypted.Checked;
            s3.RegionEndpoint = this.txtEndpoint.Text;
        }
    }
}
