﻿using CK.Plugin;
using CK.Tests.Plugin;

namespace RefExternalNonDynamicService
{

    [Plugin( PluginIdString, Version = PluginIdVersion, PublicName = PluginPublicName )]
    public class PluginSuccess : IPlugin
    {
        const string PluginIdString = "{C1901634-3619-4684-8FDB-AC166BDC9CEC}";
        const string PluginIdVersion = "1.0.0";
        const string PluginPublicName = "PluginSuccess";

        /// <summary>
        /// If the definition of INotDynamicServiceC is found, then this is perfectly valid (it is at runtime that 
        /// this plugin will fail to start if there is no such service available in the service container).
        /// If the definition is NOT FOUND, the Assembly itself can not be loaded...
        /// </summary>
        [RequiredService]
        public INotDynamicServiceC ValidRef { get; set; }

        public void Start()
        {
        }

        public void Stop()
        {
        }

        public bool Setup( IPluginSetupInfo info )
        {
            return true;
        }

        public void Teardown()
        {
        }
    }

    [Plugin( PluginIdString, Version = PluginIdVersion, PublicName = PluginPublicName )]
    public class PluginSuccessAlso : IPlugin
    {
        const string PluginIdString = "{39BB9D7D-527D-433E-91FD-5FD792B4D80B}";
        const string PluginIdVersion = "1.0.0";
        const string PluginPublicName = "PluginSuccessAlso";

        /// <summary>
        /// We allow the use of DynamicService to reference a non-dynamic interface
        /// as long as the reference is optional.
        /// </summary>
        [DynamicService( Requires = RunningRequirement.Optional )]
        public INotDynamicServiceC NotBuggyRefBecauseOtional { get; set; }

        public void Start()
        {
        }

        public void Stop()
        {
        }

        public bool Setup( IPluginSetupInfo info )
        {
            return true;
        }

        public void Teardown()
        {
        }
    }

    [Plugin( PluginIdString, Version = PluginIdVersion, PublicName = PluginPublicName )]
    public class PluginFailed : IPlugin
    {
        const string PluginIdString = "{66806DB4-E017-4249-A655-F99E32437D25}";
        const string PluginIdVersion = "1.0.0";
        const string PluginPublicName = "PluginFailed";

        /// <summary>
        /// Discover of the plugin fails since the reference must exist.
        /// </summary>
        [DynamicService( Requires = RunningRequirement.MustExist )]
        public INotDynamicServiceC BuggyRefBecauseMustExist { get; set; }

        public void Start()
        {
        }

        public void Stop()
        {
        }

        public bool Setup( IPluginSetupInfo info )
        {
            return true;
        }

        public void Teardown()
        {
        }
    }

}
