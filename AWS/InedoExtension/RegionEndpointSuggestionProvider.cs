using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Inedo.Extensibility;
using Inedo.Web;

namespace Inedo.Extensions.AWS
{
    internal sealed class RegionEndpointSuggestionProvider : ISuggestionProvider
    {
        public Task<IEnumerable<string>> GetSuggestionsAsync(IComponentConfiguration config) => Task.FromResult(RegionEndpoint.EnumerableAllRegions.Select(r => r.SystemName));
    }
}
