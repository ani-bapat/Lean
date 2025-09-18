using System;
using System.Collections.Generic;
using QuantConnect;
using QuantConnect.Data;

namespace Kohinoor.Data
{
    public class TheoBarUniverseSelectionData : BaseData
    {
        public List<Symbol> Contracts { get; set; }
        public string Underlying { get; set; }
        
        public TheoBarUniverseSelectionData()
        {
            Contracts = new List<Symbol>();
        }
        
        public override BaseData Clone()
        {
            return new TheoBarUniverseSelectionData
            {
                Time = Time,
                Symbol = Symbol,
                Contracts = new List<Symbol>(Contracts),
                Underlying = Underlying
            };
        }
        public override SubscriptionDataSource GetSource(SubscriptionDataConfig config, 
                                                         DateTime date, 
                                                         bool isLiveMode)
        {
            if (isLiveMode)
            {
                return new SubscriptionDataSource("", 
                    SubscriptionTransportMedium.Streaming);
            }
            else
            {
                return new SubscriptionDataSource("", 
                    SubscriptionTransportMedium.LocalFile);
            }
        }

    }
}