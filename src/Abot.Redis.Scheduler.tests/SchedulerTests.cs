using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Abot.Poco;
using Abot.Redis.Scheduler.Functions;
using NUnit.Framework;
using StackExchange.Redis;
using Abot.Redis.Scheduler.Types;
using Newtonsoft.Json;
using System.Dynamic;

namespace Abot.Redis.Scheduler.Tests
{
    [TestFixture]
    public class SchedulerTests
    {
        readonly string _host = "localhost";
        readonly int _port = 6379;
        readonly string _password = "";
        readonly int _database = 0;
        SchedulerState _state;

        [SetUp]
        public void SetUp()
        {
            var config = new ConfigurationOptions() { EndPoints = { { _host, _port } }, Password = _password, DefaultDatabase = _database, AllowAdmin = true };
            _state = SchedulerFunc.Create(config, "Test", _database);

            var server = _state.Connection.GetServer(_state.Connection.GetEndPoints().First());
            server.FlushDatabase(_state.Database);
        }

        [TearDown]
        public void TearDown()
        {
            _state.Connection.Close();
            _state.Connection.Dispose();
        }

        public PageToCrawl CreatePageToCrawl(string prefix, string suffix)
        {
            return new PageToCrawl(new Uri(prefix + "/" + suffix));
        }

        [Test]
        public void AddTest()
        {
            var scheduler = new RedisScheduler(_state);
            var page = CreatePageToCrawl("http://www.test.com", 1 + ".html");
            scheduler.Add(page);

            var listKey = "PageToCrawl_Test";
            var url = "CrawledPage_Test_http://www.test.com/1.html";

            var db = _state.Connection.GetDatabase();
            Assert.That(db.KeyExists(listKey), Is.True);
            Assert.That(db.KeyExists(url), Is.True);
            Assert.That(db.ListLength(listKey), Is.EqualTo(1L));
            var json = (string) db.ListGetByIndex(listKey, 0);
            var page2 = JsonConvert.DeserializeObject<PageToCrawl>(json);
            Assert.That(page.Uri, Is.EqualTo(page2.Uri));
        }

        [Test]
        public void ClearTest()
        {
            var scheduler = new RedisScheduler(_state);
            for (var i = 0; i < 100; ++i)
            {
                var page = CreatePageToCrawl("http://www.test.com/", i + ".html");
                scheduler.Add(page);
            }

            scheduler.Clear();
            Assert.That(scheduler.Count, Is.EqualTo(0));

            var server = _state.Connection.GetServer(_state.Connection.GetEndPoints().First());
            var count = server.Keys(_database, pattern: "CrawledPage_Test_*").LongCount();
            Assert.That(count, Is.EqualTo(0L));
        }
    }
}
