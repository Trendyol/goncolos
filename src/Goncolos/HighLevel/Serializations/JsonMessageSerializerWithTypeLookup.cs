using System;
using Goncolos.Consumers;
using Newtonsoft.Json.Linq;

namespace Goncolos.HighLevel.Serializations
{
    public class JsonMessageSerializerWithTypeLookup
        : JsonMessageSerializer
    {
        private readonly Func<IncomingMessage, JObject, Type> _typeResolver;

        public JsonMessageSerializerWithTypeLookup(Func<IncomingMessage, JObject, Type> typeResolver)
        {
            _typeResolver = typeResolver ?? throw new ArgumentNullException(nameof(typeResolver));
        }

        protected override Type GetMessageType(IncomingMessage incomingMessage, JObject parsedMessage)
        {
            return _typeResolver(incomingMessage, parsedMessage);
        }
    }
}