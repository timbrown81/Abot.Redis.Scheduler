using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace Abot.Redis.Scheduler.Types
{
	public struct SchedulerState
	{
		public SchedulerState(ConnectionMultiplexer connection, string siteName, int database)
		{
			Connection = connection;
			SiteName = siteName;
            Database = database;
		}

		public ConnectionMultiplexer Connection { get; }

		public string SiteName { get; }

        public int Database { get; }
	}
}
