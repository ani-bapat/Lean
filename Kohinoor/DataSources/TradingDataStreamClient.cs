using System;
using System.Buffers;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Google.Protobuf;
using Kohinoor.Protos;  // For OptionTheo
using QuantConnect.Logging;

namespace Kohinoor.DataSources
{
    public class TradingDataStreamClient(string serverAddress) : IDisposable
    {
        public void Dispose()
        {
            // Stop the receive thread
            if (_receiveThread != null && _receiveThread.IsAlive)
            {
                try { _receiveThread.Interrupt(); } catch { }
                try { _receiveThread.Join(100); } catch { }
            }
            _receiveThread = null;

            // Dispose the stream
            if (_stream != null)
            {
                try { _stream.Close(); } catch { }
                try { _stream.Dispose(); } catch { }
            }
            _stream = null;

            // Dispose the TCP client
            if (_tcpClient != null)
            {
                try { _tcpClient.Close(); } catch { }
                try { _tcpClient.Dispose(); } catch { }
            }
            _tcpClient = null;
        }

        private readonly string _serverAddress = serverAddress;
        private NetworkStream _stream;
        private Thread _receiveThread;
        public event EventHandler<OptionTheo> OnMarketDataReceived;
        private TcpClient _tcpClient;
        public bool IsConnected => _tcpClient?.Connected ?? false;

        public void StartStreaming()
        {
            // Connect to your protobuf stream source
            var parts = _serverAddress.Split(':');
            _tcpClient = new TcpClient(parts[0], int.Parse(parts[1]));
            _stream = _tcpClient.GetStream();

            _receiveThread = new Thread(ReceiveLoop) { IsBackground = true };
            _receiveThread.Start();
        }

        private int ReadVarint(NetworkStream stream)
        {
            int result = 0;
            int shift = 0;
            byte b;
            
            do
            {
                var buffer = new byte[1];
                stream.Read(buffer, 0, 1);
                b = buffer[0];
                
                result |= (b & 0x7F) << shift;
                shift += 7;
                
                if (shift >= 32)
                    throw new InvalidDataException("Varint too long");
                    
            } while ((b & 0x80) != 0);
            
            return result;
        }

        private void ReceiveLoop()
        {
            var buffer = ArrayPool<byte>.Shared.Rent(65536);
            try
            {
                while (_stream != null && _stream.CanRead)
                {
                    // Read varint length prefix
                    var messageLength = ReadVarint(_stream);
                    var bytesRead = 0;
                    while (bytesRead < messageLength)
                    {
                        bytesRead += _stream.Read(buffer, bytesRead, messageLength - bytesRead);
                    }
                    
                    // Deserialize protobuf message
                    using var memoryStream = new MemoryStream(buffer, 0, messageLength);
                    var data = OptionTheo.Parser.ParseFrom(memoryStream);
                    // Log.Debug($"Received market data: {data}");
                    OnMarketDataReceived?.Invoke(this, data);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(buffer);
            }
        }
    }
}
