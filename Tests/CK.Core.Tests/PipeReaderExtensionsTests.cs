using CK.Core.Extension;
using FluentAssertions;
using NUnit.Framework;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace CK.Core.Tests
{
    public class PipeReaderExtensionsTests
    {
        [Test]
        public async ValueTask assume_fillbuffer_iscompleted_false_if_successful()
        {
            byte[] buffer = new byte[42];
            byte[] testBuffer = new byte[42];

            using( MemoryStream stream = new MemoryStream( buffer ) )
            {
                PipeReader reader = PipeReader.Create( stream );
                ReadResult result = await reader.FillBufferAndReadAsync( testBuffer );
                result.IsCompleted.Should().BeFalse();
            }

            using( MemoryStream stream = new MemoryStream( buffer ) )
            {
                PipeReader reader = PipeReader.Create( stream );
                ReadResult result = await reader.ReadAsync();
                reader.AdvanceTo( result.Buffer.Start, result.Buffer.End );//Advance one byte.
                result = await reader.FillBufferAndReadAsync( testBuffer );
                result.IsCompleted.Should().BeFalse();
            }
        }
    }
}
