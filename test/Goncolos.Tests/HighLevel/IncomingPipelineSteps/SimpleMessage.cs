using System;

namespace Goncolos.Tests.HighLevel.IncomingPipelineSteps
{
    public class SimpleMessage
    {
        public string Text { get; set; }
        public int Number { get; set; }

        protected bool Equals(SimpleMessage other)
        {
            return Text == other.Text && Number == other.Number;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj))
            {
                return false;
            }

            if (ReferenceEquals(this, obj))
            {
                return true;
            }

            if (obj.GetType() != this.GetType())
            {
                return false;
            }

            return Equals((SimpleMessage) obj);
        }

        public override int GetHashCode()
        {
            return HashCode.Combine(Text, Number);
        }

        public static bool operator ==(SimpleMessage left, SimpleMessage right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(SimpleMessage left, SimpleMessage right)
        {
            return !Equals(left, right);
        }
    }
}