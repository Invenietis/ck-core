﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using CK.Monitoring.Server;
using CK.Core;
using System.Threading;
using System.IO;
using CK.Monitoring.Udp;
using System.Xml.Linq;

namespace CK.Monitoring.Tests
{
    [TestFixture( Category = "ActivityMonitor.Live" )]
    public class ActivityMonitorServerHostTest
    {
        [SetUp]
        public void Setup()
        {
            TestHelper.InitalizePaths();
            Directory.CreateDirectory( SystemActivityMonitor.RootLogPath );

        }

        [Test]
        public void SendLogsThroughUdp()
        {
            string grandOutputConfig = String.Format( @"
                 <GrandOutputConfiguration>
                    <Channel MinimalFilter=""Debug"">
                        <Add Type=""BinaryFile"" Name=""All-1"" Path=""Test""/>
                        <Add Type=""UdpHandler"" Name=""All-2""/>
                    </Channel>
                </GrandOutputConfiguration>" );

            GrandOutput.EnsureActiveDefault( configurator =>
            {
                GrandOutputConfiguration c = new GrandOutputConfiguration();
                Assert.That( c.Load( XDocument.Parse( grandOutputConfig ).Root, TestHelper.ConsoleMonitor ), Is.True );
                Assert.That( configurator.SetConfiguration( c, TestHelper.ConsoleMonitor ), Is.True );
            } );

            ActivityMonitor.AutoConfiguration += m =>
            {
                m.UnfilteredLog( ActivityMonitor.Tags.ApplicationSignature, LogLevel.Info, Environment.MachineName, DateTimeStamp.UtcNow, null );
            };

            ActivityMonitor monitor = new ActivityMonitor();
            for( int i = 0; i < 10000; ++i )
            {
                Thread.Sleep( 500 );

                monitor.Info().Send( Path.GetRandomFileName() );
            }
        }

        [Test]
        public void ActivityMonitorServerHostTestOpenSyncThenDispose()
        {
            using( AutoResetEvent autoResetEvent = new AutoResetEvent( false ) )
            {
                Thread t = new Thread( () =>
                {
                    ActivityMonitorServerHostConfiguration config = new ActivityMonitorServerHostConfiguration
                    {
                        Port = 3712
                    };

                    LogEntryDispatcher dispatcher = new LogEntryDispatcher();
                    ClientMonitorDatabase database = new ClientMonitorDatabase( dispatcher );

                    dispatcher.LogEntryReceived += ( sender, e ) =>
                    {
                        var appli = database.Applications.FirstOrDefault( x => x.Signature == Environment.MachineName );
                        Assert.That( appli, Is.Not.Null );
                        Assert.That( appli.Monitors.Count == 1 );

                        autoResetEvent.Set();
                    };
                    ActivityMonitorServerHost server = new ActivityMonitorServerHost( config );
                    server.Open( dispatcher.DispatchLogEntry );
                } );
                t.SetApartmentState( ApartmentState.STA );
                t.Start();

                Thread.Sleep( 1000 );

                // Client Configuration

                string grandOutputConfig = String.Format( @"
                 <GrandOutputConfiguration>
                    <Channel MinimalFilter=""Debug"">
                        <Add Type=""BinaryFile"" Name=""All-1"" Path=""Test""/>
                        <Add Type=""UdpHandler"" Name=""All-2""/>
                    </Channel>
                </GrandOutputConfiguration>" );

                GrandOutput.EnsureActiveDefault( configurator =>
                {
                    GrandOutputConfiguration c = new GrandOutputConfiguration();
                    Assert.That( c.Load( XDocument.Parse( grandOutputConfig ).Root, TestHelper.ConsoleMonitor ), Is.True );
                    Assert.That( configurator.SetConfiguration( c, TestHelper.ConsoleMonitor ), Is.True );
                } );

                ActivityMonitor.AutoConfiguration += m =>
                {
                    m.UnfilteredLog( ActivityMonitor.Tags.ApplicationSignature, LogLevel.Info, Environment.MachineName, DateTimeStamp.UtcNow, null );
                };

                Thread.Sleep( 1000 );

                ActivityMonitor monitor = new ActivityMonitor();
                monitor.Info().Send( "Test" );

                Assert.That( autoResetEvent.WaitOne( 2000 ) );
                t.Abort();
            }
        }


    }
}