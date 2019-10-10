# DataPlacement
1. Download Facebook Friendships Dataset facebook-wosn-links at: http://konect.uni-koblenz.de/networks/facebook-wosn-links
2. Download MSR Cambridge Traces at: http://iotta.snia.org/traces/388
   (Rename the trace files with Arabic numbers from 1 to 36. This represents the storage nodes from 1 to 36.)
3. Extract the request pattern information: python RequestPatterns.py
4. Extract the request rate information: python OnlineRates.py
5. Offline Community Discovery scheme: python CommunityDiscovery.py
6. Online Community Adjustment scheme: python CommunityAdjustment.py
