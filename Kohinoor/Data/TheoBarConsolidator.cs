using System;
using QuantConnect.Data;
using QuantConnect.Data.Consolidators;
using QuantConnect.Data.Market;
using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Interfaces;

namespace Kohinoor.Data
{
    // Consolidator for TheoBar to create time-based bars
    public class TheoBarConsolidator(TimeSpan period) : DataConsolidator<TheoBar>(period)
    {
        private readonly TimeSpan _period = period;
        private TheoBar _workingBar;
        private DateTime _nextBarTime;

        public override Type OutputType => typeof(TheoBar);
        
        public override IBaseData WorkingData => _workingBar;

        public override void Update(TheoBar data)
        {
            if (_workingBar == null)
            {
                StartNewBar(data);
                return;
            }
            
            if (data.Time >= _nextBarTime)
            {
                // Complete current bar and emit
                OnDataConsolidated(_workingBar);
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
                ForwardPrice = theoBar.ForwardPrice,
                DiscountFactor = theoBar.DiscountFactor,
                Z = theoBar.Z,
                OomSpread = theoBar.OomSpread,
                BsplineValues = theoBar.BsplineValues,
                Moneyness = theoBar.Moneyness,
                TimeToExpiry = theoBar.TimeToExpiry
            };
            _nextBarTime = theoBar.Time + _period;
        }
        
        private void UpdateWorkingBar(TheoBar theoBar)
        {
            // Update OHLC values for bid/ask
            _workingBar.Update(theoBar.Bid.High, theoBar.Bid.Low, theoBar.Bid.Close,
                            theoBar.Ask.High, theoBar.Ask.Low, theoBar.Ask.Close,
                            theoBar.LastBidSize, theoBar.LastAskSize);
            
            // Update end time and latest values
            _workingBar.EndTime = theoBar.EndTime;
            _workingBar.TheoreticalValue = theoBar.TheoreticalValue;
            _workingBar.Greeks = theoBar.Greeks;
            _workingBar.ImpliedVolatility = theoBar.ImpliedVolatility;
            _workingBar.OpenInterest = theoBar.OpenInterest;
            
            // Keep latest values for additional fields
            _workingBar.ForwardPrice = theoBar.ForwardPrice;
            _workingBar.DiscountFactor = theoBar.DiscountFactor;
            _workingBar.Z = theoBar.Z;
            _workingBar.OomSpread = theoBar.OomSpread;
            _workingBar.BsplineValues = theoBar.BsplineValues;
            _workingBar.TimeToExpiry = theoBar.TimeToExpiry;
        }
    }
}