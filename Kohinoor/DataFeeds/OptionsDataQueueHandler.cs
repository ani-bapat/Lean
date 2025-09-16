using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using QuantConnect;
using QuantConnect.Data;
using QuantConnect.Interfaces;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Packets;
using QuantConnect.Data.Consolidators;
using Kohinoor.Data;
using Kohinoor.DataSources;
using QuantConnect.Logging;
using QuantConnect.Configuration;

namespace Kohinoor.DataFeeds 
{
    public class OptionsDataQueueHandler : IDataQueueHandler, IDisposable
    {
        private readonly ProtobufStreamProcessor _streamProcessor;
        private readonly ConcurrentDictionary<Symbol, SubscriptionDataConfig> _subscriptions;
        private readonly ConcurrentDictionary<Symbol, IDataConsolidator> _consolidators;
        private readonly ConcurrentDictionary<Symbol, Queue<TheoBar>> _dataQueues;

        public bool IsConnected => _streamProcessor != null && _streamProcessor.IsConnected;
        
        // Implement Dispose
        public void Dispose()
        {
            _streamProcessor?.Dispose();
            
            foreach (var consolidator in _consolidators.Values)
            {
                consolidator?.Dispose();
            }
            _consolidators.Clear();
            _subscriptions.Clear();
            _dataQueues.Clear();
        }

        public OptionsDataQueueHandler()
        {
            Console.WriteLine("OptionsDataQueueHandler: Constructor called");
            Log.Trace("OptionsDataQueueHandler: Initializing...");
            
            _subscriptions = new ConcurrentDictionary<Symbol, SubscriptionDataConfig>();
            _consolidators = new ConcurrentDictionary<Symbol, IDataConsolidator>();
            _dataQueues = new ConcurrentDictionary<Symbol, Queue<TheoBar>>();
            
            var theo_server = Config.Get("theo-server", "localhost:50051");
            Log.Trace($"OptionsDataQueueHandler: Connecting to {theo_server}");
            
            try
            {
                _streamProcessor = new ProtobufStreamProcessor(theo_server);
                _streamProcessor.OnTheoBarReceived += OnRawTheoBarReceived;
                Log.Trace("OptionsDataQueueHandler: Successfully initialized");
            }
            catch (Exception ex)
            {
                Log.Error($"OptionsDataQueueHandler: Failed to initialize - {ex.Message}");
                throw;
            }
        }        

        public IEnumerator<BaseData> Subscribe(SubscriptionDataConfig dataConfig, 
                                            EventHandler newDataAvailableHandler)
        {
            _subscriptions.TryAdd(dataConfig.Symbol, dataConfig);
            Log.Debug($"Received data config {dataConfig}");
            // Create consolidator for this symbol with the specified resolution
            var consolidator = new TheoBarConsolidator(dataConfig.Resolution.ToTimeSpan());
            consolidator.DataConsolidated += (sender, consolidated) =>
            {
                var queue = _dataQueues.GetOrAdd(dataConfig.Symbol, _ => new Queue<TheoBar>());
                lock (queue)
                {
                    queue.Enqueue((TheoBar)consolidated);
                }
                newDataAvailableHandler?.Invoke(this, EventArgs.Empty);
            };
            
            _consolidators.TryAdd(dataConfig.Symbol, consolidator);
            _dataQueues.TryAdd(dataConfig.Symbol, new Queue<TheoBar>());
            Log.Debug($"Consolidator {_consolidators}");
            return GetDataEnumerator(dataConfig.Symbol);
        }
        
        private void OnRawTheoBarReceived(object sender, TheoBar theoBar)
        {
            Log.Debug($"Received RawTheoBar: {theoBar}");
            // Send raw data to appropriate consolidator
            if (_consolidators.TryGetValue(theoBar.Symbol, out var consolidator))
            {
                consolidator.Update(theoBar);
            }
        }
        
        private IEnumerator<BaseData> GetDataEnumerator(Symbol symbol)
        {
            while (true)
            {
                if (_dataQueues.TryGetValue(symbol, out var queue))
                {
                    lock (queue)
                    {
                        while (queue.Count > 0)
                        {
                            yield return queue.Dequeue();
                        }
                    }
                }
                Thread.Sleep(10); // Small delay to prevent tight loop
            }
        }
        
        public void Unsubscribe(SubscriptionDataConfig dataConfig)
        {
            _subscriptions.TryRemove(dataConfig.Symbol, out _);
            if (_consolidators.TryRemove(dataConfig.Symbol, out var consolidator))
            {
                consolidator.Dispose();
            }
            _dataQueues.TryRemove(dataConfig.Symbol, out _);
        }
        
        public void SetJob(LiveNodePacket job)
        {
            // Configure any job-specific parameters
            // _streamProcessor.StartStreaming();
        }
    }
}