using System.Threading;
using System.Threading.Tasks;

namespace Goncolos.Consumers
{
    public delegate Task OnMessageReceived(IncomingMessage[] incomingMessages, CancellationToken cancellationToken);
}