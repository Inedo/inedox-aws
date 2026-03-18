using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Amazon;
using Inedo.Extensibility;
using Inedo.Web;

namespace Inedo.Extensions.AWS;

internal sealed class RegionEndpointSuggestionProvider : ISuggestionProvider
{
    public IAsyncEnumerable<string> GetSuggestionsAsync(IComponentConfiguration config, CancellationToken cancellationToken) => RegionEndpoint.EnumerableAllRegions.Select(r => r.SystemName).ToAsyncEnumerable();
}
