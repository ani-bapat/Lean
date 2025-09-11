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

    private void ReceiveLoop()
    {
        var buffer = ArrayPool<byte>.Shared.Rent(65536);
        try
        {
            while (_stream != null && _stream.CanRead)
            {
                // Read message length prefix (assuming length-prefixed messages)
                var lengthBuffer = new byte[4];
                _stream.Read(lengthBuffer, 0, 4);
                var messageLength = BitConverter.ToInt32(lengthBuffer, 0);
                
                // Read the protobuf message
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
