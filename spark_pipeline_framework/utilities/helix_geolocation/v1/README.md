This package, which we call the service in the rest of this document, is designed around location services. In the future, the standardization module could move to a separate sub-module if it makes sense.
## How standardization works
At this moment, we use [Mellisa Address API](https://www.melissa.com/developer/global-address) as the vendor to get a standard version of an address. In addition, the Latitude and Longitude of the location is also part of the response. 
### Steps
1. Consumer calls `StandardizeAddr.standardize` with a set of mail addresses
2. The service checks the cache to see if any of the addresses are existed (meaning have been queried before)
3. The service request address standardization from the vendor (e.g. Melissa) for the rest of the addresses. Note: Mellisa returns the input address if it was unable to resolve the address
4. The service saves the vendor responses to the cache for future reference.
5. The service returns the standardized address (or the input address in case of match failure by the vendor)

### design goals
We have the following considerations in mind:
- Cache should not be accessed directly by the consumers of the API 
- Over time we might use different vendors, so we save Vendor in the cache
- A vendor might change the response format so we save the version of the response in the cache

### Credentials 
When run on server credentials can be found on [SSM](https://console.aws.amazon.com/systems-manager/parameters/?region=us-east-1&tab=Table): `/prod/helix/external/melissa/`

### Tech stack
- Mellisa: an HTTP API to be called by POST requests
- Cache: MongoDB on local and DocumentDB while on Dev or Prod environments
