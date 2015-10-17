using System.Diagnostics;
using System.Dynamic;
using System.Threading;
using Abot.Poco;
using Abot.Redis.Scheduler.Types;
using Newtonsoft.Json;

namespace Abot.Redis.Scheduler.Functions
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using System.Text;
	using System.Threading.Tasks;
	using StackExchange.Redis;

	public static class SchedulerFunc
	{
		public static SchedulerState Create(string address, int port, string password, int database, string siteName)
		{
			var options = new ConfigurationOptions() { EndPoints = {{address, port}}, Password = password};
			return Create(options, siteName, database);
		}

		public static SchedulerState Create(ConfigurationOptions config, string siteName, int database)
		{
			var connection = ConnectionMultiplexer.Connect(config);
			return new SchedulerState(connection, siteName, database);
		}

		public static IDatabase GetDatabase(SchedulerState state)
		{
			return state.Connection.GetDatabase();
		}

		public static ITransaction CreateTransaction(SchedulerState scheduler)
		{
			return scheduler.Connection.GetDatabase().CreateTransaction();
		}

		public static string CrawledPageKey(string siteName, string url)
		{
			return $"CrawledPage_{siteName}_{url}";
		}

		public static string PageToCrawlKey(string siteName)
		{
			return $"PageToCrawl_{siteName}";
		}

		public static int Count(SchedulerState state)
		{
			var count = GetDatabase(state).ListLengthAsync(PageToCrawlKey(state.SiteName)).Result;
			return Convert.ToInt32(count);
		}

		public static void Add(SchedulerState state, PageToCrawl page)
		{
			var json = JsonConvert.SerializeObject(page);
			var url = page.Uri.AbsoluteUri;
			var trans = CreateTransaction(state);
			var crawledPageKey = CrawledPageKey(state.SiteName, url);
			var pageToCrawlKey = PageToCrawlKey(state.SiteName);
			trans.AddCondition(Condition.KeyNotExists(crawledPageKey));
			trans.StringSetAsync(crawledPageKey, "");
			trans.ListLeftPushAsync(pageToCrawlKey, json);
			trans.ExecuteAsync().Wait();
		}

		private static PageToCrawl GetNextInner(SchedulerState state, int attempt)
		{
			if (attempt > 10) return null;

			var pageToCrawlKey = PageToCrawlKey(state.SiteName);
			var trans = CreateTransaction(state);
			trans.AddCondition(Condition.KeyExists(pageToCrawlKey));
			var taskJson = trans.ListRightPopAsync(pageToCrawlKey);
			var committed = trans.ExecuteAsync().Result;
			if (!committed)
			{
				Thread.Sleep(500);
				return GetNextInner(state, attempt + 1);
			}
			var json = taskJson.Result;
			var pageToCrawl = JsonConvert.DeserializeObject<PageToCrawl>(json);
			pageToCrawl.PageBag = new ExpandoObject();
			return pageToCrawl;
		}

		public static PageToCrawl GetNext(SchedulerState state)
		{
			return GetNextInner(state, 1);
		}

		public static void Clear(SchedulerState state)
		{
            var database = state.Connection.GetDatabase();
            var server = state.Connection.GetServer(state.Connection.GetEndPoints().First());
            foreach(var key in server.Keys(state.Database, pattern: CrawledPageKey(state.SiteName, "*")))
            {
                database.KeyDelete(key);
            }
            database.KeyDelete(PageToCrawlKey(state.SiteName));
		}
	}
}
