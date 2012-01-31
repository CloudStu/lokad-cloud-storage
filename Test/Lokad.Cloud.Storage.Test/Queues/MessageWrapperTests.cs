#region Copyright (c) Lokad 2009-2011
// This code is released under the terms of the new BSD licence.
// URL: http://www.lokad.com/
#endregion

using System.IO;
using NUnit.Framework;

namespace Lokad.Cloud.Storage.Test.Queues
{
    [TestFixture]
    public class MessageWrapperTests
    {
        [Test]
        public void Serialization()
        {
            // overflowing message
            var om = new OverflowWrapper {ContainerName = "con", BlobName = "blo"};

            var stream = new MemoryStream();
            var serializer = new CloudFormatter();

            serializer.Serialize(om, stream, om.GetType());
            stream.Position = 0;
            var omBis = (OverflowWrapper) serializer.Deserialize(stream, typeof (OverflowWrapper));

            Assert.AreEqual(om.ContainerName, omBis.ContainerName, "#A00");
            Assert.AreEqual(om.BlobName, omBis.BlobName, "#A01");
        }
    }
}
