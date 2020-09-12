#nullable enable
using System;
using System.Buffers;
using System.ComponentModel;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace CK.Core.Extension
{
    public static class PipeReaderExtensions
    {
        /// <summary>
        /// Asynchronously reads a sequence of at least <paramref name="minimumByteCount"/> bytes  from this <see cref="PipeReader"/>.
        /// Asking big <paramref name="minimumByteCount"/> will make the circular buffer grow, avoid doing that.
        /// </summary>
        /// <param name="reader">This instance to read from.</param>
        /// <param name="minimumByteCount">Minimum number of bytes that must be available in the buffered sequence reader.</param>
        /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
        /// <returns>The <see cref="ReadResult"/> that exposes the buffer.</returns>
        /// <remarks>
        /// The buffer size may be smaller if:
        /// <list type="bullet">
        ///     <item>The operation was cancelled by the <paramref name="cancellationToken"/>, you can check it with the flag <see cref="ReadResult.IsCanceled"/>.</item>
        ///     <item>The pipe was completed before reading all asked bytes, you can check it with the flag <see cref="ReadResult.IsCompleted"/>.</item>
        /// </list>
        /// </remarks>
        [MethodImpl( MethodImplOptions.AggressiveInlining )]
        public static async ValueTask<ReadResult> ReadAsync( this PipeReader reader, int minimumByteCount, CancellationToken cancellationToken = default )
        {
            ReadResult result = await reader.ReadAsync( cancellationToken );
            // Loop until the result's Buffer has the requested bytes.
            while( result.Buffer.Length < minimumByteCount )
            {
                if( result.IsCanceled || result.IsCompleted )
                {
                    // Completed/Canceled: we can't read any more bytes, so we return.
                    return result;
                }
                // We need to signal that we examined the data, so the next read will fetch more data. If we don't the next read won't wait for more data.
                reader.AdvanceTo( result.Buffer.Start, result.Buffer.End );
                result = await reader.ReadAsync( cancellationToken );
            }
            return result;
        }

        /// <summary>
        /// Fills the a byte buffer with the data from this <see cref="PipeReader"/>.
        /// </summary>
        /// <param name="reader">This <see cref="PipeReader"/> to read data from.</param>
        /// <param name="buffer">The buffer to fill.</param>
        /// <param name="cancellationToken">A cancellation token to cancel the operation.</param>
        /// <returns>The current read result.</returns>
        public static async ValueTask<ReadResult> FillBufferAndReadAsync( this PipeReader reader, Memory<byte> buffer, CancellationToken cancellationToken = default )
        {
            while( true )
            {
                ReadResult result = await reader.ReadAsync( cancellationToken );
                ReadOnlySequence<byte> readBuffer = result.Buffer;
                // Slices the pipe buffer if bigger than target.
                if( readBuffer.Length > buffer.Length ) readBuffer = readBuffer.Slice( 0, buffer.Length );
                // Copies data to the input buffer...
                readBuffer.CopyTo( buffer.Span );
                //... and truncates the input buffer, so we don't overwrite our data.
                buffer = buffer.Slice( (int)readBuffer.Length );
                // And don't forget to signal that we consumed the data.
                reader.AdvanceTo( readBuffer.End );

                if( buffer.Length == 0
                    || result.IsCompleted
                    || result.IsCanceled || cancellationToken.IsCancellationRequested )
                {
                    return new ReadResult( result.Buffer.Slice( readBuffer.Length ),
                                           result.IsCanceled || cancellationToken.IsCancellationRequested,
                                           result.IsCompleted
                    );
                }
            }
        }
    }
}
