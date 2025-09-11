using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using QuantConnect;
using QuantConnect.Data.Market;
using QuantConnect.Logging;
using Kohinoor.Data;
using Kohinoor.Protos;  // For OptionTheo

namespace Kohinoor.DataSources
{
    // Processes protobuf stream messages into TheoBar instances
    public class ProtobufStreamProcessor : IDisposable
    {
        private readonly TradingDataStreamClient _streamClient;
        private readonly ConcurrentQueue<OptionTheo> _messageQueue;
        private readonly Timer _processingTimer;
        private readonly CancellationTokenSource _cancellationTokenSource;

        public event EventHandler<TheoBar> OnTheoBarReceived;
        public bool IsConnected => _streamClient?.IsConnected ?? false;

        public ProtobufStreamProcessor(string serverAddress)
        {
            _messageQueue = new ConcurrentQueue<OptionTheo>();
            _cancellationTokenSource = new CancellationTokenSource();
            _streamClient = new TradingDataStreamClient(serverAddress);

            // Process queued messages every 500ms to maintain timing
            _processingTimer = new Timer(ProcessMessageBatch, null, 0, 500);

            _streamClient.OnMarketDataReceived += OnProtobufDataReceived;
        }

        public void Dispose()
        {
            _cancellationTokenSource?.Cancel();
            _cancellationTokenSource?.Dispose();
            _processingTimer?.Dispose();
            _streamClient?.Dispose();
        }

        private void OnProtobufDataReceived(object sender, OptionTheo data)
        {
            // Queue message for batch processing to avoid blocking stream
            _messageQueue.Enqueue(data);
        }

        private void ProcessMessageBatch(object state)
        {
            var processedCount = 0;
            var stopwatch = Stopwatch.StartNew();

            // Process messages with time budget (leave 50ms buffer)
            while (_messageQueue.TryDequeue(out var protobufData) &&
                stopwatch.ElapsedMilliseconds < 450 &&
                processedCount < 1000) // Max batch size
            {
                try
                {
                    var theoBar = ConvertToTheoBar(protobufData);
                    OnTheoBarReceived?.Invoke(this, theoBar);
                    processedCount++;
                }
                catch (Exception ex)
                {
                    Log.Error($"Error processing protobuf data: {ex.Message}");
                }
            }
        }

        private TheoBar ConvertToTheoBar(OptionTheo data)
        {
            var symbol = Symbol.CreateOption(data.UnderlyingSymbol, Market.USA,
                                            OptionStyle.European, data.OptionRight,
                                            data.Strike, data.Expiry);

            var bid = new Bar(data.BidPrice, data.BidPrice, data.BidPrice, data.BidPrice);
            var ask = new Bar(data.AskPrice, data.AskPrice, data.AskPrice, data.AskPrice);

            var greeks = new Greeks(
                data.Delta, data.Gamma, data.Vega, data.Theta, data.Rho
            );

            return new TheoBar(
                data.Timestamp.ToDateTime(),
                symbol,
                bid, data.BidSize,
                ask, data.AskSize,
                data.TheoreticalValue,
                greeks,
                data.ImpliedVolatility,
                data.OpenInterest
            );
        }
    }
}