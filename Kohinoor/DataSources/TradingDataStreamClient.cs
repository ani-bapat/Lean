using System;
using System.Buffers;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Google.Protobuf;
using Kohinoor.Protos;  // For OptionTheo

namespace Kohinoor.DataSources
{
    public class TradingDataStreamClient(string serverAddress)
    {
        private readonly string _serverAddress = serverAddress;
        private NetworkStream _stream;
        private Thread _receiveThread;
        public bool IsConnected => _tcpClient?.Connected ?? false;    
        public event EventHandler<OptionTheo> OnMarketDataReceived;

        public void StartStreaming()
        {
            // Connect to your protobuf stream source
            var parts = _serverAddress.Split(':');
            var tcpClient = new TcpClient(parts[0], int.Parse(parts[1]));
            _stream = tcpClient.GetStream();
            
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
