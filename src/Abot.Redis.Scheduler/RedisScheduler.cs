using System.Collections.Generic;
using Abot.Core;
using Abot.Poco;
using Abot.Redis.Scheduler.Functions;
using Abot.Redis.Scheduler.Types;

namespace Abot.Redis.Scheduler
{
    public class RedisScheduler : IScheduler
	{
        private readonly SchedulerState _state;

		public RedisScheduler(SchedulerState state)
		{
			_state = state;
		}

		public int Count {
			get
			{
				var count = SchedulerFunc.Count(_state);
				return count;
			}
		}

		public void Add(PageToCrawl page)
		{			
			SchedulerFunc.Add(_state, page);
		}

		public void Add(IEnumerable<PageToCrawl> pages)
		{
			foreach(var page in pages)
				Add(page);
		}

		public PageToCrawl GetNext()
		{
			var page = SchedulerFunc.GetNext(_state);
			return page;
		}

		public void Clear()
		{
			SchedulerFunc.Clear(_state);
		}

        public void Dispose() 
        {
            _state.Connection.Dispose();
        }
    }
}
