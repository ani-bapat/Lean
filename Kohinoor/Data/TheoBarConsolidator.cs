using System;
using QuantConnect.Data;
using QuantConnect.Data.Consolidators;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Interfaces;
using QuantConnect.Logging;

namespace Kohinoor.Data
{
    // Consolidator for TheoBar to create time-based bars
    public class TheoBarConsolidator : DataConsolidator<TheoBar>
    {
        private readonly TimeSpan _period;
        private TheoBar _workingBar;
        private DateTime _nextBarTime;

        public override Type OutputType => typeof(TheoBar);
        
        public override IBaseData WorkingData => _workingBar;

        public TheoBarConsolidator(TimeSpan period)
        {
            _period = period;
        }

        public override void Update(TheoBar data)
        {
            Log.Debug($"Received TheoBar: {data}");
            if (_workingBar == null)
            {
                Log.Debug($"Starting new TheoBar: {data}");
                StartNewBar(data);
                return;
            }
            
            if (data.Time >= _nextBarTime)
            {
                // Complete current bar and emit
                OnDataConsolidated(_workingBar);
                Log.Debug($"Consolidated TheoBar: {_workingBar}");
                StartNewBar(data);
            }
            else
            {
                // Update current working bar
                UpdateWorkingBar(data);
            }
        }

        public override void Scan(DateTime currentLocalTime)
        {
            if (_workingBar != null && currentLocalTime >= _nextBarTime)
            {
                OnDataConsolidated(_workingBar);
                _workingBar = null;
            }
        }       

        private void StartNewBar(TheoBar theoBar)
        {
            _workingBar = new TheoBar(
                theoBar.Time, theoBar.Symbol,
                theoBar.Bid, theoBar.LastBidSize,
                theoBar.Ask, theoBar.LastAskSize,
                theoBar.TheoreticalValue, theoBar.Greeks,
                theoBar.ImpliedVolatility, theoBar.OpenInterest,
                _period
            )
            {
                Forward = theoBar.Forward,
                Discount = theoBar.Discount,
                Moneyness = theoBar.Moneyness,
                TradingTimeToExpiry = theoBar.TradingTimeToExpiry
            };
            _nextBarTime = theoBar.Time + _period;
        }
        
        private void UpdateWorkingBar(TheoBar theoBar)
        {
            // Update OHLC values for bid/ask
            _workingBar.Bid.Update(theoBar.Bid.Close);
            _workingBar.Ask.Update(theoBar.Ask.Close);

            // Update end time and latest values
            _workingBar.EndTime = theoBar.EndTime;
            _workingBar.TheoreticalValue = theoBar.TheoreticalValue;
            _workingBar.Greeks = theoBar.Greeks;
            _workingBar.ImpliedVolatility = theoBar.ImpliedVolatility;
            _workingBar.OpenInterest = theoBar.OpenInterest;
            
            // Keep latest values for additional fields
            _workingBar.Forward = theoBar.Forward;
            _workingBar.Discount = theoBar.Discount;
            _workingBar.TradingTimeToExpiry = theoBar.TradingTimeToExpiry;
            _workingBar.Settlement = theoBar.Settlement;
            _workingBar.Moneyness = theoBar.Moneyness;
        }
    }
}