using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using QuantConnect;
using QuantConnect.Logging;
using Kohinoor.Data;
using Kohinoor.Universe;

namespace Kohinoor.DataSources
{
    public class OptionsUniverseDataFeed : IDisposable
    {
        private readonly ConcurrentDictionary<string, HashSet<Symbol>> _availableContracts;
        private readonly ProtobufStreamProcessor _streamProcessor;
        
        public event EventHandler<OptionsChainUpdate> OnChainUpdate;
        
        public OptionsUniverseDataFeed(string serverAddress)
        {
            _availableContracts = new ConcurrentDictionary<string, HashSet<Symbol>>();
            _streamProcessor = new ProtobufStreamProcessor(serverAddress);
            _streamProcessor.OnTheoBarReceived += HandleIncomingTheoBar;
        }
        
        private void HandleIncomingTheoBar(object sender, TheoBar theoBar)
        {
            // Extract underlying from symbol (e.g., "SPX" from SPX option symbol)
            var underlying = GetUnderlyingFromSymbol(theoBar.Symbol);
            var contracts = _availableContracts.GetOrAdd(underlying, _ => new HashSet<Symbol>());
            
            bool isNew = false;
            lock (contracts)
            {
                isNew = contracts.Add(theoBar.Symbol);
            }
            
            if (isNew)
            {
                Log.Trace($"New contract discovered: {theoBar.Symbol}");
                OnChainUpdate?.Invoke(this, new OptionsChainUpdate 
                { 
                    Underlying = underlying, 
                    Contracts = contracts.ToList(),
                    TheoBar = theoBar 
                });
            }
        }
        
        private string GetUnderlyingFromSymbol(Symbol symbol)
        {
            // For options, get the underlying symbol
            if (symbol.HasUnderlying)
            {
                return symbol.Underlying.Value;
            }
            
            // Parse from symbol string if needed
            var parts = symbol.Value.Split('_');
            return parts.Length > 0 ? parts[0] : symbol.Value;
        }
        
        public IEnumerable<Symbol> GetAvailableContracts(string underlying)
        {
            if (_availableContracts.TryGetValue(underlying, out var contracts))
            {
                return contracts.ToList();
            }
            return Enumerable.Empty<Symbol>();
        }
        
        public void StartStreaming()
        {
            _streamProcessor.StartStreaming();
        }
        
        public void Dispose()
        {
            _streamProcessor?.Dispose();
        }
    }
}